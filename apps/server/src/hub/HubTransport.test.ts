import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  type ExecutionEnvironmentDescriptor,
  HubWorkerFrame,
  HubArchiveBatch,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Socket from "effect/socket/Socket";
import * as TestClock from "effect/testing/TestClock";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as HubArchive from "./HubArchive.ts";
import * as HubConfiguration from "./HubConfiguration.ts";
import * as HubTransport from "./HubTransport.ts";

const descriptor = (id: string): ExecutionEnvironmentDescriptor => ({
  environmentId: EnvironmentId.make(id),
  label: id,
  platform: { os: "linux", arch: "x64" },
  serverVersion: "test",
  capabilities: { repositoryIdentity: false },
});
const codec = Schema.fromJsonString(Schema.toCodecJson(HubWorkerFrame));
const encodeFrame = Schema.encodeEffect(codec);
const decodeFrame = Schema.decodeEffect(codec);
const encodeBatch = Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(HubArchiveBatch)));

const fixture = Effect.gen(function* () {
  const values = new Map<string, Uint8Array>();
  const received: HubArchiveBatch[] = [];
  const secrets = Layer.succeed(ServerSecretStore.ServerSecretStore, {
    get: (key) => Effect.sync(() => Option.fromUndefinedOr(values.get(key))),
    set: (key, value) =>
      Effect.sync(() => {
        values.set(key, value);
      }),
    create: () => Effect.void,
    remove: (key) =>
      Effect.sync(() => {
        values.delete(key);
      }),
    getOrCreateRandom: () => Effect.succeed(new Uint8Array()),
  });
  const archive = Layer.succeed(HubArchive.HubArchive, {
    receiveBatch: (_id, batch) =>
      Effect.sync(() => {
        received.push(batch);
        return { storeId: batch.storeId, ackSequence: batch.throughSequence };
      }),
    status: () => Effect.die("unused"),
    reportError: () => Effect.void,
    shell: () => Effect.die("unused"),
    thread: () => Effect.die("unused"),
    history: () => Effect.die("unused"),
    item: () => Effect.die("unused"),
    asset: () => Effect.die("unused"),
  });
  const environment = Layer.succeed(ServerEnvironment.ServerEnvironment, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("hub")),
    getDescriptor: Effect.succeed(descriptor("hub")),
  });
  const layer = HubTransport.layer.pipe(
    Layer.provide(HubConfiguration.layer),
    Layer.provide(archive),
    Layer.provide(environment),
    Layer.provide(secrets),
    Layer.provide(NodeServices.layer),
  );
  const context = yield* Layer.build(layer);
  return { context, layer, values, received };
});

it.effect(
  "consumes enrollment once, hashes credentials, retains machine identity and revokes replaced workers",
  () =>
    Effect.gen(function* () {
      const { context, layer, values } = yield* fixture;
      yield* Effect.gen(function* () {
        const hub = yield* HubTransport.HubTransport;
        const enrollment = yield* hub.createEnrollment({ label: "Laptop" });
        expect(new TextDecoder().decode(values.get("hub-transport-state"))).not.toContain(
          enrollment.code,
        );
        const attempts = yield* Effect.forEach(
          [1, 2],
          () =>
            hub
              .enroll({ code: enrollment.code, environment: descriptor("worker") })
              .pipe(Effect.result),
          { concurrency: "unbounded" },
        );
        const succeeded = attempts.filter((attempt) => attempt._tag === "Success");
        expect(succeeded).toHaveLength(1);
        const first = succeeded[0]!;
        if (first._tag !== "Success") return;
        expect(new TextDecoder().decode(values.get("hub-transport-state"))).not.toContain(
          first.success.workerToken,
        );
        yield* hub.authorizeWorker(first.success.workerId, first.success.workerToken);
        expect(
          (yield* hub.authorizeWorker(first.success.workerId, "wrong").pipe(Effect.result))._tag,
        ).toBe("Failure");
        const replacement = yield* hub.createEnrollment({});
        expect(
          (yield* hub
            .enroll({ code: replacement.code, environment: descriptor("worker") })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(yield* hub.remove(EnvironmentId.make("worker"))).toBe(true);
        const second = yield* hub.enroll({
          code: replacement.code,
          environment: descriptor("worker"),
        });
        expect(
          (yield* hub
            .authorizeWorker(first.success.workerId, first.success.workerToken)
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        yield* hub.authorizeWorker(second.workerId, second.workerToken);
        expect(yield* hub.list).toHaveLength(1);
        const restarted = yield* Layer.build(layer);
        expect(
          yield* HubTransport.HubTransport.pipe(
            Effect.flatMap((hub) => hub.list),
            Effect.provide(restarted),
          ),
        ).toEqual(yield* hub.list);
        expect(yield* hub.remove(EnvironmentId.make("worker"))).toBe(true);
        expect(
          (yield* hub.authorizeWorker(second.workerId, second.workerToken).pipe(Effect.result))
            ._tag,
        ).toBe("Failure");
      }).pipe(Effect.provide(context));
    }),
);

it.effect("expires unused enrollment and refuses enrolling the hub itself", () =>
  Effect.gen(function* () {
    const { context } = yield* fixture;
    yield* Effect.gen(function* () {
      const hub = yield* HubTransport.HubTransport;
      const enrollment = yield* hub.createEnrollment({});
      expect(
        (yield* hub
          .enroll({ code: enrollment.code, environment: descriptor("hub") })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      yield* TestClock.adjust("10 minutes");
      expect(
        (yield* hub
          .enroll({ code: enrollment.code, environment: descriptor("worker") })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(yield* hub.list).toEqual([]);
    }).pipe(Effect.provide(context));
  }),
);

it.effect("refuses a worker removed while its socket writer is being acquired", () =>
  Effect.gen(function* () {
    const { context } = yield* fixture;
    yield* Effect.gen(function* () {
      const hub = yield* HubTransport.HubTransport;
      const environment = descriptor("worker");
      const enrollment = yield* hub.createEnrollment({});
      const enrolled = yield* hub.enroll({ code: enrollment.code, environment });
      const acquiringWriter = yield* Deferred.make<void>();
      const writerReady = yield* Deferred.make<void>();
      const socket = Socket.make({
        reader: Effect.die("A removed worker must not activate its socket reader"),
        writer: Deferred.succeed(acquiringWriter, undefined).pipe(
          Effect.andThen(Deferred.await(writerReady)),
          Effect.as({ write: () => Effect.void, writeAll: () => Effect.void }),
        ),
      });
      const connection = yield* hub
        .attach(enrolled.workerId, enrolled.workerToken, socket)
        .pipe(Effect.forkScoped);
      yield* Deferred.await(acquiringWriter);
      expect(yield* hub.remove(environment.environmentId)).toBe(true);
      yield* Deferred.succeed(writerReady, undefined);
      const rejected = yield* Fiber.join(connection).pipe(Effect.result);
      expect(rejected._tag === "Failure" && rejected.failure.reason).toBe("unauthorized");
      const offline = yield* hub
        .requestWorker(environment.environmentId, "probe", {})
        .pipe(Effect.result);
      expect(offline._tag === "Failure" && offline.failure.reason).toBe("offline");
    }).pipe(Effect.provide(context));
  }),
);

it.effect(
  "routes responses, acknowledges an archive only after storing it, and disconnects removed workers",
  () =>
    Effect.gen(function* () {
      const { context, received } = yield* fixture;
      yield* Effect.gen(function* () {
        const hub = yield* HubTransport.HubTransport;
        const environment = descriptor("worker");
        const enrollment = yield* hub.createEnrollment({});
        const enrolled = yield* hub.enroll({ code: enrollment.code, environment });
        const incoming = yield* Queue.unbounded<string>();
        const outgoing = yield* Queue.unbounded<string>();
        const attached = yield* Deferred.make<void>();
        const socket = Socket.make({
          reader: Deferred.succeed(attached, undefined).pipe(
            Effect.as({
              pull: Queue.take(incoming).pipe(Effect.map((frame) => [frame] as const)),
              upgrade: () => Effect.void,
            }),
          ),
          writer: Effect.succeed({
            write: (frame) =>
              Queue.offer(outgoing, typeof frame === "string" ? frame : "").pipe(Effect.asVoid),
            writeAll: (frames) =>
              Effect.forEach(
                frames,
                (frame) => Queue.offer(outgoing, typeof frame === "string" ? frame : ""),
                { discard: true },
              ),
          }),
        });
        const connection = yield* hub
          .attach(enrolled.workerId, enrolled.workerToken, socket)
          .pipe(Effect.forkScoped);
        yield* Deferred.await(attached);
        expect((yield* hub.list)[0]?.online).toBe(true);
        const request = yield* hub
          .requestWorker(environment.environmentId, "probe", {})
          .pipe(Effect.forkScoped);
        const message = yield* decodeFrame(yield* Queue.take(outgoing));
        if (message.type !== "request") return yield* Effect.die("Expected request");
        yield* Queue.offer(
          incoming,
          yield* encodeFrame({
            type: "response",
            requestId: message.requestId,
            success: false,
            payload: { errorCode: "conflict" },
          }),
        );
        const rejected = yield* Fiber.join(request).pipe(Effect.result);
        expect(rejected._tag === "Failure" && rejected.failure.workerCode).toBe("conflict");
        const batch = {
          storeId: "source",
          afterSequence: 0,
          throughSequence: 0,
          events: [],
          complete: true,
        } satisfies HubArchiveBatch;
        const json = yield* encodeBatch(batch);
        const split = Math.floor(json.length / 2);
        yield* Queue.offer(
          incoming,
          yield* encodeFrame({
            type: "archive.chunk",
            requestId: "batch",
            index: 0,
            total: 2,
            data: json.slice(0, split),
          }),
        );
        yield* Queue.offer(
          incoming,
          yield* encodeFrame({
            type: "archive.chunk",
            requestId: "batch",
            index: 1,
            total: 2,
            data: json.slice(split),
          }),
        );
        expect(yield* decodeFrame(yield* Queue.take(outgoing))).toEqual({
          type: "archive.ack",
          requestId: "batch",
          ack: { storeId: "source", ackSequence: 0 },
        });
        expect(received).toEqual([batch]);
        yield* hub.remove(environment.environmentId);
        yield* Fiber.join(connection);
        expect(yield* hub.list).toEqual([]);
      }).pipe(Effect.provide(context));
    }),
);
