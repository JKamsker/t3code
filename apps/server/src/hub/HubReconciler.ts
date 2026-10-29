import * as Crypto from "effect/Crypto";
import { HubConfigurationApply, HubObservedConfiguration } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as ConfigurationWrites from "./ConfigurationWrites.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectService from "../project/ProjectService.ts";
import { mapSettingsSecrets, SECRET_REDACTED } from "../settingsSecrets.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";

export class HubReconciliationError extends Schema.TaggedError<HubReconciliationError>()(
  "HubReconciliationError",
  {
    reason: Schema.Literals(["conflict", "apply", "recovery"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return "Machine could not apply hub configuration.";
  }
}
export class HubReconciler extends Context.Service<
  HubReconciler,
  {
    readonly snapshot: Effect.Effect<HubObservedConfiguration, HubReconciliationError>;
    readonly apply: (
      input: HubConfigurationApply,
      scope?: string,
    ) => Effect.Effect<void, HubReconciliationError>;
  }
>()("t3/hub/HubReconciler") {}

const ResourceReceipt = Schema.Struct({ baselineHash: Schema.String, appliedHash: Schema.String });
const decodeResourceReceipt = Schema.decodeEffect(Schema.fromJsonString(ResourceReceipt));
const encodeResourceReceipt = Schema.encodeEffect(Schema.fromJsonString(ResourceReceipt));
const PreparedApply = Schema.Struct({ intentHash: Schema.String, beforeHash: Schema.String });
const decodePreparedApply = Schema.decodeEffect(Schema.fromJsonString(PreparedApply));
const encodePreparedApply = Schema.encodeEffect(Schema.fromJsonString(PreparedApply));
const encodeUnknown = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const isReconciliationError = Schema.is(HubReconciliationError);
const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const settings = yield* ServerSettings.ServerSettingsService;
  const projects = yield* ProjectService.ProjectService;
  const tasks = yield* ScheduledTasks.ScheduledTaskService;
  const lock = yield* Semaphore.make(1);
  const hash = Effect.fn("HubReconciler.hash")((value: unknown) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(encodeUnknown(value)))
      .pipe(Effect.map((bytes) => Buffer.from(bytes).toString("hex"))),
  );
  const snapshot = Effect.gen(function* () {
    const materialized = yield* settings.getSettings;
    const current = mapSettingsSecrets(materialized, (secret) =>
      secret.sensitive ? { value: SECRET_REDACTED, redacted: true } : secret,
    );
    const projectSnapshot = yield* projects.snapshot;
    const automation = yield* tasks.list();
    const hashes: Record<string, string> = { settings: yield* hash(materialized) };
    for (const project of projectSnapshot.projects)
      hashes[`project:${project.id}`] = yield* hash(project);
    for (const task of automation.tasks) {
      const {
        id,
        title,
        prompt,
        enabled,
        schedule,
        projectId,
        threadId,
        workspaceStrategy,
        modelSelection,
        runtimeMode,
        interactionMode,
      } = task;
      hashes[`automation:${id}`] = yield* hash({
        id,
        title,
        prompt,
        enabled,
        schedule,
        projectId,
        threadId,
        workspaceStrategy,
        modelSelection,
        runtimeMode,
        interactionMode,
      });
    }
    return {
      settings: current,
      projects: projectSnapshot,
      automation,
      hashes,
    };
  }).pipe(Effect.mapError((cause) => new HubReconciliationError({ reason: "apply", cause })));
  const apply = Effect.fn("HubReconciler.apply")(
    function* (input: HubConfigurationApply, scope?: string) {
      const receiptKey = `hub-configuration-receipt-${yield* hash(input.requestId)}`;
      const intentHash = yield* hash(input.intent);
      const receipt = yield* secrets.get(receiptKey);
      if (Option.isSome(receipt)) {
        if (new TextDecoder().decode(receipt.value) !== intentHash)
          return yield* new HubReconciliationError({ reason: "conflict" });
        return;
      }
      const resourceKey =
        input.intent.type.startsWith("project.") && "projectId" in input.intent
          ? `project:${input.intent.projectId}`
          : input.intent.type.startsWith("automation.") && "input" in input.intent
            ? `automation:${input.intent.input.id ?? ""}`
            : "settings";
      const resourceReceiptKey =
        scope === undefined
          ? null
          : `hub-configuration-resource-${yield* hash(`${scope}:${resourceKey}`)}`;
      const preparedKey = `${receiptKey}-prepared`;
      const preparedBytes = yield* secrets.get(preparedKey);
      const before = yield* snapshot;
      const beforeHash = before.hashes[resourceKey] ?? (yield* hash(null));
      if (Option.isSome(preparedBytes)) {
        const prepared = yield* decodePreparedApply(new TextDecoder().decode(preparedBytes.value));
        if (prepared.intentHash !== intentHash)
          return yield* new HubReconciliationError({ reason: "conflict" });
        if (prepared.beforeHash !== beforeHash)
          return yield* new HubReconciliationError({ reason: "recovery" });
      }
      let baselineHash = input.expectedHash;
      if (input.expectedHash !== undefined) {
        const current = before;
        const currentHash = current.hashes[resourceKey] ?? (yield* hash(null));
        if (currentHash !== input.expectedHash) {
          const saved =
            resourceReceiptKey === null
              ? Option.none<Uint8Array>()
              : yield* secrets.get(resourceReceiptKey);
          const previous = Option.isSome(saved)
            ? yield* decodeResourceReceipt(new TextDecoder().decode(saved.value))
            : null;
          if (
            previous === null ||
            previous.baselineHash !== input.expectedHash ||
            previous.appliedHash !== currentHash
          ) {
            return yield* new HubReconciliationError({ reason: "conflict" });
          }
          baselineHash = previous.baselineHash;
        }
      }
      yield* secrets.set(
        preparedKey,
        new TextEncoder().encode(yield* encodePreparedApply({ intentHash, beforeHash })),
      );
      const intent = input.intent;
      switch (intent.type) {
        case "settings.patch":
          yield* settings.updateSettings(intent.patch);
          break;
        case "provider.mutate":
          yield* settings.updateProviderInstance(intent.mutation);
          break;
        case "project.create":
          yield* projects.create(intent);
          break;
        case "project.update":
          yield* projects.update(intent);
          break;
        case "project.delete":
          yield* projects.delete({
            commandId: intent.commandId,
            projectId: intent.projectId,
            ...(intent.force === undefined ? {} : { force: intent.force }),
          });
          break;
        case "automation.upsert":
          yield* tasks.upsert(intent.input);
          break;
        case "automation.delete":
          yield* tasks.delete(intent.input);
          break;
        case "automation.setEnabled":
          yield* tasks.setEnabled(intent.input);
          break;
      }
      if (resourceReceiptKey !== null && baselineHash !== undefined) {
        const current = yield* snapshot;
        const appliedHash = current.hashes[resourceKey] ?? (yield* hash(null));
        yield* secrets.set(
          resourceReceiptKey,
          new TextEncoder().encode(yield* encodeResourceReceipt({ baselineHash, appliedHash })),
        );
      }
      yield* secrets.set(receiptKey, new TextEncoder().encode(intentHash));
      yield* secrets.remove(preparedKey);
    },
    ConfigurationWrites.atomic,
    lock.withPermit,
    Effect.mapError((cause) =>
      isReconciliationError(cause) ? cause : new HubReconciliationError({ reason: "apply", cause }),
    ),
  );
  return HubReconciler.of({ apply, snapshot });
});
export const layer = Layer.effect(HubReconciler, make);
