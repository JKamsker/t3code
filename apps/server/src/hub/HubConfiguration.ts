import * as Crypto from "effect/Crypto";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import {
  EnvironmentId,
  HubConfigurationApply,
  HubConfigurationEntry,
  HubConfigurationEnqueue,
  HubConfigurationIntent,
  HubConfigurationState,
  HubObservedConfiguration,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";

export class HubConfigurationError extends Schema.TaggedError<HubConfigurationError>()(
  "HubConfigurationError",
  {
    reason: Schema.Literals(["storage", "conflict", "request-conflict", "capacity"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Hub configuration ${this.reason}.`;
  }
}

const isConfigurationError = Schema.is(HubConfigurationError);
const StoredEntry = Schema.Struct({
  ...HubConfigurationEntry.fields,
  intent: Schema.NullOr(HubConfigurationIntent),
  intentHash: Schema.String,
  expectedHash: Schema.optional(Schema.String),
});
const StoredState = Schema.Record(
  Schema.String,
  Schema.Struct({ revision: Schema.Number, entries: Schema.Array(StoredEntry) }),
);
type StoredState = typeof StoredState.Type;
const encode = Schema.encodeEffect(Schema.fromJsonString(StoredState));
const encodeIntent = Schema.encodeEffect(Schema.fromJsonString(HubConfigurationIntent));
const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(StoredState));

export class HubConfiguration extends Context.Service<
  HubConfiguration,
  {
    readonly observed: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<HubObservedConfiguration | null, HubConfigurationError>;
    readonly observe: (
      environmentId: EnvironmentId,
      snapshot: HubObservedConfiguration,
    ) => Effect.Effect<void, HubConfigurationError>;
    readonly list: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<HubConfigurationState, HubConfigurationError>;
    readonly enqueue: (
      input: HubConfigurationEnqueue,
    ) => Effect.Effect<HubConfigurationState, HubConfigurationError>;
    readonly cancel: (
      environmentId: EnvironmentId,
      requestId: string,
    ) => Effect.Effect<HubConfigurationState, HubConfigurationError>;
    readonly reconcile: <E>(
      environmentId: EnvironmentId,
      apply: (input: HubConfigurationApply) => Effect.Effect<void, E>,
    ) => Effect.Effect<HubConfigurationState, HubConfigurationError>;
  }
>()("t3/hub/HubConfiguration") {}

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const lock = yield* Semaphore.make(1);
  const reconciliationLocks = yield* KeyedLock.make<EnvironmentId>();
  const read = Effect.fn("HubConfiguration.read")(
    function* () {
      const stored = yield* secrets.get("hub-desired-configuration");
      return Option.isSome(stored)
        ? yield* decode(new TextDecoder().decode(stored.value))
        : ({} as StoredState);
    },
    Effect.mapError((cause) => new HubConfigurationError({ reason: "storage", cause })),
  );
  const write = Effect.fn("HubConfiguration.write")(
    function* (state: StoredState) {
      yield* secrets.set(
        "hub-desired-configuration",
        new TextEncoder().encode(yield* encode(state)),
      );
    },
    Effect.mapError((cause) => new HubConfigurationError({ reason: "storage", cause })),
  );
  const summarize = (state: StoredState[string] | undefined): HubConfigurationState => ({
    revision: state?.revision ?? 0,
    entries:
      state?.entries.map(
        ({ intent: _intent, intentHash: _hash, expectedHash: _expectedHash, ...entry }) => entry,
      ) ?? [],
  });
  const list = Effect.fn("HubConfiguration.list")(function* (environmentId: EnvironmentId) {
    return summarize((yield* read())[environmentId]);
  }, lock.withPermit);
  const enqueue = Effect.fn("HubConfiguration.enqueue")(
    function* (input: HubConfigurationEnqueue) {
      const state = yield* read();
      const current = state[input.environmentId] ?? { revision: 0, entries: [] };
      const intent: HubConfigurationIntent =
        input.intent.type === "automation.upsert"
          ? {
              ...input.intent,
              input: {
                ...input.intent.input,
                id:
                  input.intent.input.id ??
                  (`hub-${input.requestId}` as NonNullable<typeof input.intent.input.id>),
                commandId: `hub-${input.requestId}` as NonNullable<
                  typeof input.intent.input.commandId
                >,
              },
            }
          : input.intent;
      const intentHash = Buffer.from(
        yield* crypto.digest("SHA-256", new TextEncoder().encode(yield* encodeIntent(intent))),
      ).toString("hex");
      const duplicate = current.entries.find((entry) => entry.requestId === input.requestId);
      if (duplicate !== undefined) {
        if (duplicate.intentHash !== intentHash) {
          return yield* new HubConfigurationError({ reason: "request-conflict" });
        }
        return summarize(current);
      }
      if (input.expectedRevision !== current.revision)
        return yield* new HubConfigurationError({ reason: "conflict" });
      if (current.entries.length >= 10000)
        return yield* new HubConfigurationError({ reason: "capacity" });
      const next = {
        revision: current.revision + 1,
        entries: [
          ...current.entries,
          {
            requestId: input.requestId,
            ...(input.expectedHash === undefined ? {} : { expectedHash: input.expectedHash }),
            revision: current.revision + 1,
            intent,
            intentHash,
            type: intent.type,
            status: "pending" as const,
            error: null,
          },
        ],
      };
      yield* write({ ...state, [input.environmentId]: next });
      return summarize(next);
    },
    lock.withPermit,
    Effect.mapError((cause) =>
      isConfigurationError(cause) ? cause : new HubConfigurationError({ reason: "storage", cause }),
    ),
  );
  const cancel = Effect.fn("HubConfiguration.cancel")(
    function* (environmentId: EnvironmentId, requestId: string) {
      const state = yield* read();
      const current = state[environmentId];
      if (current === undefined) return summarize(undefined);
      const entries = current.entries.map((entry) =>
        entry.requestId === requestId && entry.status !== "applied"
          ? { ...entry, intent: null, status: "cancelled" as const, error: null }
          : entry,
      );
      const next = { ...current, entries };
      yield* write({ ...state, [environmentId]: next });
      return summarize(next);
    },
    lock.withPermit,
    (effect, environmentId) => reconciliationLocks.withLock(environmentId, effect),
  );
  const reconcile = Effect.fn("HubConfiguration.reconcile")(
    function* <E>(
      environmentId: EnvironmentId,
      apply: (input: HubConfigurationApply) => Effect.Effect<void, E>,
    ) {
      const pending = yield* read().pipe(lock.withPermit);
      for (const entry of pending[environmentId]?.entries ?? []) {
        if (entry.intent === null || entry.status === "applied" || entry.status === "cancelled")
          continue;
        const result = yield* apply({
          requestId: entry.requestId,
          intent: entry.intent,
          ...(entry.expectedHash === undefined ? {} : { expectedHash: entry.expectedHash }),
        }).pipe(Effect.result);
        const offline =
          result._tag === "Failure" &&
          typeof result.failure === "object" &&
          result.failure !== null &&
          "reason" in result.failure &&
          result.failure.reason === "offline";
        if (offline) break;
        yield* Effect.gen(function* () {
          const state = yield* read();
          const current = state[environmentId]!;
          const entries = current.entries.map((latest) =>
            latest.requestId === entry.requestId && latest.status !== "cancelled"
              ? {
                  ...latest,
                  intent: result._tag === "Success" ? null : latest.intent,
                  status: result._tag === "Success" ? ("applied" as const) : ("failed" as const),
                  error:
                    result._tag === "Success"
                      ? null
                      : typeof result.failure === "object" &&
                          result.failure !== null &&
                          "workerCode" in result.failure &&
                          result.failure.workerCode === "conflict"
                        ? "Configuration changed on the machine. Refresh its settings and submit the edit again."
                        : typeof result.failure === "object" &&
                            result.failure !== null &&
                            "workerCode" in result.failure &&
                            result.failure.workerCode === "recovery"
                          ? "This change may already have applied before the connection was interrupted. Review the machine's reported settings, then cancel this change and submit a new edit if needed."
                          : "Machine could not apply this configuration. Reconnect it and retry the change.",
                }
              : latest,
          );
          yield* write({ ...state, [environmentId]: { ...current, entries } });
        }).pipe(lock.withPermit);
        if (result._tag === "Failure") break;
      }
      return yield* list(environmentId);
    },
    (effect, environmentId) => reconciliationLocks.withLock(environmentId, effect),
  );
  const observedCodec = Schema.fromJsonString(HubObservedConfiguration);
  const observedKey = (id: EnvironmentId) =>
    `hub-observed-${Buffer.from(id).toString("base64url")}`;
  const observed = Effect.fn("HubConfiguration.observed")(
    function* (id: EnvironmentId) {
      const value = yield* secrets.get(observedKey(id));
      return Option.isNone(value)
        ? null
        : yield* Schema.decodeEffect(observedCodec)(new TextDecoder().decode(value.value));
    },
    Effect.mapError((cause) => new HubConfigurationError({ reason: "storage", cause })),
  );
  const observe = Effect.fn("HubConfiguration.observe")(
    function* (id: EnvironmentId, snapshot: HubObservedConfiguration) {
      yield* secrets.set(
        observedKey(id),
        new TextEncoder().encode(yield* Schema.encodeEffect(observedCodec)(snapshot)),
      );
    },
    Effect.mapError((cause) => new HubConfigurationError({ reason: "storage", cause })),
  );
  return HubConfiguration.of({ list, enqueue, cancel, reconcile, observed, observe });
});
export const layer = Layer.effect(HubConfiguration, make);
