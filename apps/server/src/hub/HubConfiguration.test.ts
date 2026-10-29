import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as HubConfiguration from "./HubConfiguration.ts";

const fixture = Effect.gen(function* () {
  const values = new Map<string, Uint8Array>();
  const secretLayer = Layer.succeed(ServerSecretStore.ServerSecretStore, {
    get: (key) => Effect.sync(() => Option.fromUndefinedOr(values.get(key))),
    set: (key, value) =>
      Effect.sync(() => {
        values.set(key, value);
      }),
    remove: (key) =>
      Effect.sync(() => {
        values.delete(key);
      }),
    create: () => Effect.void,
    getOrCreateRandom: () => Effect.succeed(new Uint8Array()),
  });
  const context = yield* HubConfiguration.layer.pipe(
    Layer.provide(Layer.merge(secretLayer, NodeServices.layer)),
    Layer.build,
  );
  return { context, secretLayer, values };
});
const environmentId = EnvironmentId.make("machine-1");
const input = {
  environmentId,
  expectedRevision: 0,
  requestId: "change-1",
  intent: { type: "settings.patch" as const, patch: {} },
};

it.effect(
  "durably queues configuration with CAS and request replay without disclosing the payload",
  () =>
    Effect.gen(function* () {
      const { context } = yield* fixture;
      yield* Effect.gen(function* () {
        const config = yield* HubConfiguration.HubConfiguration;
        expect((yield* config.enqueue(input)).revision).toBe(1);
        expect((yield* config.enqueue(input)).revision).toBe(1);
        const conflict = yield* config
          .enqueue({ ...input, requestId: "change-2" })
          .pipe(Effect.result);
        expect(conflict._tag).toBe("Failure");
        expect(yield* config.list(environmentId)).toEqual({
          revision: 1,
          entries: [
            {
              requestId: "change-1",
              revision: 1,
              type: "settings.patch",
              status: "pending",
              error: null,
            },
          ],
        });
      }).pipe(Effect.provide(context));
    }),
);

it.effect(
  "stops after a failed dependency and resumes in order without repeating acknowledged changes",
  () =>
    Effect.gen(function* () {
      const { context } = yield* fixture;
      yield* Effect.gen(function* () {
        const config = yield* HubConfiguration.HubConfiguration;
        yield* config.enqueue(input);
        yield* config.enqueue({ ...input, expectedRevision: 1, requestId: "change-2" });
        const offline = yield* config.reconcile(environmentId, () =>
          Effect.fail({ reason: "offline" }),
        );
        expect(offline.entries.map((entry) => entry.status)).toEqual(["pending", "pending"]);
        const failed = yield* config.reconcile(environmentId, () => Effect.fail("invalid"));
        expect(failed.entries.map((entry) => entry.status)).toEqual(["failed", "pending"]);
        const applied: string[] = [];
        yield* config.reconcile(environmentId, (entry) =>
          Effect.sync(() => {
            applied.push(entry.requestId);
          }),
        );
        yield* config.reconcile(environmentId, (entry) =>
          Effect.sync(() => {
            applied.push(entry.requestId);
          }),
        );
        expect(applied).toEqual(["change-1", "change-2"]);
      }).pipe(Effect.provide(context));
    }),
);

it.effect("keeps pending edits across service restart and removes delivered secret payloads", () =>
  Effect.gen(function* () {
    const { context, secretLayer, values } = yield* fixture;
    yield* Effect.gen(function* () {
      const config = yield* HubConfiguration.HubConfiguration;
      yield* config.enqueue(input);
    }).pipe(Effect.provide(context));
    const restarted = yield* HubConfiguration.layer.pipe(
      Layer.provide(Layer.merge(secretLayer, NodeServices.layer)),
      Layer.build,
    );
    yield* Effect.gen(function* () {
      const config = yield* HubConfiguration.HubConfiguration;
      expect((yield* config.list(environmentId)).entries[0]?.status).toBe("pending");
      yield* config.reconcile(environmentId, () => Effect.void);
      expect((yield* config.list(environmentId)).entries[0]?.status).toBe("applied");
      const persisted = new TextDecoder().decode(values.get("hub-desired-configuration"));
      expect(persisted).toContain('"intent":null');
      expect(persisted).not.toContain('"patch"');
      expect((yield* config.enqueue(input)).revision).toBe(1);
    }).pipe(Effect.provide(restarted));
  }),
);

it.effect("cancels pending work durably and does not dispatch it", () =>
  Effect.gen(function* () {
    const { context } = yield* fixture;
    yield* Effect.gen(function* () {
      const config = yield* HubConfiguration.HubConfiguration;
      yield* config.enqueue(input);
      expect((yield* config.cancel(environmentId, input.requestId)).entries[0]?.status).toBe(
        "cancelled",
      );
      let calls = 0;
      yield* config.reconcile(environmentId, () =>
        Effect.sync(() => {
          calls++;
        }),
      );
      expect(calls).toBe(0);
    }).pipe(Effect.provide(context));
  }),
);
