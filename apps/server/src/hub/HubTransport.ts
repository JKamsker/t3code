// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import {
  EnvironmentId,
  ExecutionEnvironmentDescriptor,
  HubArchiveBatch,
  HubWorkerFrame,
  HubObservedConfiguration,
  HubConfigurationApply,
  type HubEnrollRequest,
  type HubEnrollResult,
  type HubEnrollmentResult,
  type HubEnvironmentSummary,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Socket from "effect/socket/Socket";
import { HttpServerRequest, HttpServerResponse } from "effect/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as HubArchive from "./HubArchive.ts";
import * as HubConfiguration from "./HubConfiguration.ts";

export class HubTransportError extends Schema.TaggedError<HubTransportError>()(
  "HubTransportError",
  {
    reason: Schema.Literals([
      "enrollment",
      "unauthorized",
      "missing",
      "offline",
      "storage",
      "protocol",
      "timeout",
      "worker",
    ]),
    cause: Schema.optional(Schema.Defect()),
    workerCode: Schema.optional(Schema.Literals(["conflict", "apply", "invalid", "recovery"])),
  },
) {
  override get message() {
    return `Hub operation failed (${this.reason}).`;
  }
}

const StoredWorker = Schema.Struct({
  workerId: Schema.String,
  tokenHash: Schema.String,
  environment: ExecutionEnvironmentDescriptor,
  lastSeenAt: Schema.NullOr(Schema.String),
});
const StoredState = Schema.Struct({
  workers: Schema.Array(StoredWorker),
  enrollments: Schema.Array(
    Schema.Struct({
      enrollmentId: Schema.String,
      codeHash: Schema.String,
      expiresAt: Schema.Number,
      label: Schema.optionalKey(Schema.String),
    }),
  ),
});
type StoredState = typeof StoredState.Type;
const decodeState = Schema.decodeUnknownEffect(Schema.fromJsonString(StoredState));
const encodeState = Schema.encodeEffect(Schema.fromJsonString(StoredState));
const frameCodec = Schema.fromJsonString(Schema.toCodecJson(HubWorkerFrame));
const decodeFrame = Schema.decodeUnknownEffect(frameCodec);
const encodeFrame = Schema.encodeEffect(frameCodec);
const decodeBatch = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.toCodecJson(HubArchiveBatch)),
);
const isHubTransportError = Schema.is(HubTransportError);
const decodeObservedConfiguration = Schema.decodeUnknownEffect(HubObservedConfiguration);
const encodeConfigurationApply = Schema.encodeEffect(Schema.toCodecJson(HubConfigurationApply));
const decodeWorkerFailure = Schema.decodeUnknownOption(
  Schema.Struct({ errorCode: Schema.Literals(["conflict", "apply", "invalid", "recovery"]) }),
);
const decodeArchiveStatusRequest = Schema.decodeUnknownOption(
  Schema.Struct({ storeId: Schema.String }),
);
const decodeArchiveErrorRequest = Schema.decodeUnknownOption(
  Schema.Struct({ reason: Schema.Literals(["source-changed", "cursor-mismatch", "storage"]) }),
);
const digest = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const matches = (value: string, hash: string) =>
  NodeCrypto.timingSafeEqual(Buffer.from(digest(value)), Buffer.from(hash));

type ActiveWorker = {
  readonly send: (frame: HubWorkerFrame) => Effect.Effect<void, HubTransportError>;
  readonly channels: Map<string, Queue.Queue<HubWorkerFrame>>;
  readonly requests: Map<string, Deferred.Deferred<unknown, HubTransportError>>;
  readonly closed: Deferred.Deferred<void>;
};

export class HubTransport extends Context.Service<
  HubTransport,
  {
    readonly list: Effect.Effect<ReadonlyArray<HubEnvironmentSummary>, HubTransportError>;
    readonly createEnrollment: (input: {
      readonly label?: string;
    }) => Effect.Effect<HubEnrollmentResult, HubTransportError>;
    readonly enroll: (input: HubEnrollRequest) => Effect.Effect<HubEnrollResult, HubTransportError>;
    readonly remove: (environmentId: EnvironmentId) => Effect.Effect<boolean, HubTransportError>;
    readonly descriptor: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<ExecutionEnvironmentDescriptor, HubTransportError>;
    readonly authorizeWorker: (
      workerId: string,
      token: string,
    ) => Effect.Effect<void, HubTransportError>;
    readonly attach: (
      workerId: string,
      token: string,
      socket: Socket.Socket,
    ) => Effect.Effect<void, HubTransportError, Scope.Scope>;
    readonly requestWorker: (
      environmentId: EnvironmentId,
      method: string,
      payload: unknown,
    ) => Effect.Effect<unknown, HubTransportError>;
    readonly proxy: (
      environmentId: EnvironmentId,
      path: string,
      scopes: ReadonlyArray<AuthEnvironmentScope>,
      request: HttpServerRequest.HttpServerRequest,
    ) => Effect.Effect<HttpServerResponse.HttpServerResponse, HubTransportError, Scope.Scope>;
  }
>()("t3/hub/HubTransport") {}

const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const archive = yield* HubArchive.HubArchive;
  const configuration = yield* HubConfiguration.HubConfiguration;
  const lock = yield* Semaphore.make(1);
  const active = new Map<EnvironmentId, ActiveWorker>();
  const read = Effect.fn("HubTransport.read")(
    function* () {
      const value = yield* secrets.get("hub-transport-state");
      return Option.isNone(value)
        ? ({ workers: [], enrollments: [] } satisfies StoredState)
        : yield* decodeState(new TextDecoder().decode(value.value));
    },
    Effect.mapError((cause) => new HubTransportError({ reason: "storage", cause })),
  );
  const write = Effect.fn("HubTransport.write")(
    function* (state: StoredState) {
      yield* secrets.set(
        "hub-transport-state",
        new TextEncoder().encode(yield* encodeState(state)),
      );
    },
    Effect.mapError((cause) => new HubTransportError({ reason: "storage", cause })),
  );
  const workerFor = Effect.fn("HubTransport.workerFor")(function* (environmentId: EnvironmentId) {
    const worker = (yield* read()).workers.find(
      (worker) => worker.environment.environmentId === environmentId,
    );
    if (!worker) return yield* new HubTransportError({ reason: "missing" });
    return worker;
  });
  const list = Effect.map(read(), (state) =>
    state.workers.map((worker) => ({
      environmentId: worker.environment.environmentId,
      label: worker.environment.label,
      environment: worker.environment,
      online: active.has(worker.environment.environmentId),
      lastSeenAt: worker.lastSeenAt,
    })),
  );
  const createEnrollment = Effect.fn("HubTransport.createEnrollment")(function* (input: {
    readonly label?: string;
  }) {
    const state = yield* read();
    const now = yield* Clock.currentTimeMillis;
    const code = NodeCrypto.randomBytes(24).toString("base64url");
    const enrollmentId = NodeCrypto.randomUUID();
    const expiresAt = now + 10 * 60_000;
    yield* write({
      ...state,
      enrollments: [
        ...state.enrollments.filter((entry) => entry.expiresAt > now),
        { enrollmentId, codeHash: digest(code), expiresAt, ...input },
      ],
    });
    return { enrollmentId, code, expiresAt: DateTime.formatIso(DateTime.makeUnsafe(expiresAt)) };
  }, lock.withPermit);
  const enroll = Effect.fn("HubTransport.enroll")(function* (input: HubEnrollRequest) {
    const state = yield* read();
    const now = yield* Clock.currentTimeMillis;
    const enrollment = state.enrollments.find(
      (entry) => entry.expiresAt > now && matches(input.code, entry.codeHash),
    );
    if (!enrollment) return yield* new HubTransportError({ reason: "enrollment" });
    const hubId = yield* environment.getEnvironmentId;
    if (
      input.environment.environmentId === hubId ||
      state.workers.some(
        (worker) => worker.environment.environmentId === input.environment.environmentId,
      )
    )
      return yield* new HubTransportError({ reason: "enrollment" });
    const workerId = NodeCrypto.randomUUID();
    const workerToken = NodeCrypto.randomBytes(32).toString("base64url");
    const descriptor = { ...input.environment, label: enrollment.label ?? input.environment.label };
    yield* write({
      workers: [
        ...state.workers,
        { workerId, tokenHash: digest(workerToken), environment: descriptor, lastSeenAt: null },
      ],
      enrollments: state.enrollments.filter(
        (entry) => entry.enrollmentId !== enrollment.enrollmentId,
      ),
    });
    return { hubId, workerId, workerToken };
  }, lock.withPermit);
  const remove = Effect.fn("HubTransport.remove")(function* (environmentId: EnvironmentId) {
    const state = yield* read();
    const workers = state.workers.filter(
      (worker) => worker.environment.environmentId !== environmentId,
    );
    if (workers.length === state.workers.length) return false;
    yield* write({ ...state, workers });
    const worker = active.get(environmentId);
    if (worker) {
      active.delete(environmentId);
      yield* Deferred.succeed(worker.closed, undefined);
    }
    return true;
  }, lock.withPermit);
  const descriptor = Effect.fn("HubTransport.descriptor")(function* (environmentId: EnvironmentId) {
    return (yield* workerFor(environmentId)).environment;
  });
  const requestWorker = Effect.fn("HubTransport.requestWorker")(function* (
    environmentId: EnvironmentId,
    method: string,
    payload: unknown,
  ) {
    const worker = active.get(environmentId);
    if (!worker) return yield* new HubTransportError({ reason: "offline" });
    const requestId = NodeCrypto.randomUUID();
    const result = yield* Deferred.make<unknown, HubTransportError>();
    worker.requests.set(requestId, result);
    return yield* worker.send({ type: "request", requestId, method, payload }).pipe(
      Effect.andThen(Deferred.await(result)),
      Effect.timeout("30 seconds"),
      Effect.catchTags({
        TimeoutError: (cause) => Effect.fail(new HubTransportError({ reason: "timeout", cause })),
      }),
      Effect.ensuring(Effect.sync(() => worker.requests.delete(requestId))),
    );
  });
  const attach = Effect.fn("HubTransport.attach")(
    function* (workerId: string, token: string, socket: Socket.Socket) {
      const saved = (yield* read()).workers.find(
        (worker) => worker.workerId === workerId && matches(token, worker.tokenHash),
      );
      if (!saved) return yield* new HubTransportError({ reason: "unauthorized" });
      const environmentId = saved.environment.environmentId;
      const writer = yield* socket.writer;
      const closed = yield* Deferred.make<void>();
      const worker: ActiveWorker = {
        send: (frame) =>
          encodeFrame(frame).pipe(
            Effect.flatMap((text) => writer.write(text)),
            Effect.mapError((cause) => new HubTransportError({ reason: "protocol", cause })),
          ),
        channels: new Map(),
        requests: new Map(),
        closed,
      };
      let lastReceivedAt = yield* Clock.currentTimeMillis;
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          if (active.get(environmentId) === worker) active.delete(environmentId);
          for (const request of worker.requests.values())
            yield* Deferred.fail(request, new HubTransportError({ reason: "offline" }));
          for (const queue of worker.channels.values()) yield* Queue.shutdown(queue);
        }),
      );
      yield* Effect.gen(function* () {
        const current = (yield* read()).workers.find(
          (entry) => entry.workerId === workerId && matches(token, entry.tokenHash),
        );
        if (!current) return yield* new HubTransportError({ reason: "unauthorized" });
        const previous = active.get(environmentId);
        if (previous) yield* Deferred.succeed(previous.closed, undefined);
        active.set(environmentId, worker);
      }).pipe(lock.withPermit);
      const chunkState = new Map<
        string,
        { readonly total: number; readonly parts: Array<string>; size: number }
      >();
      const saveDescriptor = Effect.fn("HubTransport.saveDescriptor")(function* (
        descriptor: ExecutionEnvironmentDescriptor,
      ) {
        if (descriptor.environmentId !== environmentId)
          return yield* new HubTransportError({ reason: "protocol" });
        const state = yield* read();
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* write({
          ...state,
          workers: state.workers.map((entry) =>
            entry.workerId === workerId
              ? {
                  ...entry,
                  environment: { ...descriptor, label: entry.environment.label },
                  lastSeenAt: now,
                }
              : entry,
          ),
        });
      }, lock.withPermit);
      const archiveBatch = Effect.fn("HubTransport.archiveBatch")(function* (
        requestId: string,
        batch: HubArchiveBatch,
      ) {
        const ack = yield* archive.receiveBatch(environmentId, batch).pipe(
          Effect.tapError((cause) =>
            archive.reportError(
              environmentId,
              cause.reason === "invalid-batch" ? "storage" : cause.reason,
            ),
          ),
          Effect.mapError((cause) => new HubTransportError({ reason: "storage", cause })),
        );
        yield* worker.send({ type: "archive.ack", requestId, ack });
      });
      const workerRequest = Effect.fn("HubTransport.workerRequest")(function* (
        frame: Extract<HubWorkerFrame, { type: "request" }>,
      ) {
        if (frame.method === "hub.archive.status") {
          const input = decodeArchiveStatusRequest(frame.payload);
          if (Option.isSome(input)) {
            const status = yield* archive.status(environmentId);
            if (status.storeId !== null && status.storeId !== input.value.storeId)
              yield* archive.reportError(environmentId, "source-changed");
            return yield* worker.send({
              type: "response",
              requestId: frame.requestId,
              success: true,
              payload: yield* archive.status(environmentId),
            });
          }
        } else if (frame.method === "hub.archive.error") {
          const input = decodeArchiveErrorRequest(frame.payload);
          if (Option.isSome(input)) {
            yield* archive.reportError(environmentId, input.value.reason);
            return yield* worker.send({
              type: "response",
              requestId: frame.requestId,
              success: true,
              payload: { reported: true },
            });
          }
        }
        yield* worker.send({
          type: "response",
          requestId: frame.requestId,
          success: false,
          payload: { errorCode: "invalid" },
        });
      });
      const handle = Effect.fn("HubTransport.handleFrame")(function* (frame: HubWorkerFrame) {
        switch (frame.type) {
          case "hello":
            yield* saveDescriptor(frame.environment);
            yield* Effect.gen(function* () {
              const state = yield* configuration.reconcile(environmentId, (input) =>
                encodeConfigurationApply(input).pipe(
                  Effect.flatMap((payload) =>
                    requestWorker(environmentId, "hub.configuration.apply", payload),
                  ),
                  Effect.asVoid,
                ),
              );
              yield* requestWorker(environmentId, "hub.configuration.ready", {
                ready: !state.entries.some(
                  (entry) => entry.status === "pending" || entry.status === "failed",
                ),
              });
              const snapshot = yield* requestWorker(
                environmentId,
                "hub.configuration.snapshot",
                null,
              ).pipe(Effect.flatMap(decodeObservedConfiguration));
              yield* configuration.observe(environmentId, snapshot);
            }).pipe(Effect.ignore, Effect.forkScoped);
            break;
          case "ping":
            yield* worker.send({ type: "pong" });
            break;
          case "pong":
            break;
          case "response": {
            const request = worker.requests.get(frame.requestId);
            if (request) {
              const code = decodeWorkerFailure(frame.payload);
              yield* frame.success
                ? Deferred.succeed(request, frame.payload)
                : Deferred.fail(
                    request,
                    new HubTransportError({
                      reason: "worker",
                      ...(Option.isSome(code) ? { workerCode: code.value.errorCode } : {}),
                    }),
                  );
            }
            break;
          }
          case "request":
            yield* workerRequest(frame).pipe(
              Effect.catch(() =>
                worker.send({
                  type: "response",
                  requestId: frame.requestId,
                  success: false,
                  payload: { errorCode: "apply" },
                }),
              ),
              Effect.forkScoped,
            );
            break;
          case "archive.batch":
            yield* archiveBatch(frame.requestId, frame.batch);
            break;
          case "archive.chunk": {
            if (
              frame.total < 1 ||
              frame.total > 1024 ||
              frame.data.length > 262_144 ||
              (frame.index === 0 && chunkState.size > 0)
            )
              return yield* new HubTransportError({ reason: "protocol" });
            let current = chunkState.get(frame.requestId);
            if (!current && frame.index === 0) {
              current = { total: frame.total, parts: [], size: 0 };
              chunkState.set(frame.requestId, current);
            }
            if (
              !current ||
              frame.index !== current.parts.length ||
              current.total !== frame.total ||
              current.size + frame.data.length > 128 * 1024 * 1024
            )
              return yield* new HubTransportError({ reason: "protocol" });
            current.parts.push(frame.data);
            current.size += frame.data.length;
            if (current.parts.length === current.total) {
              chunkState.delete(frame.requestId);
              const batch = yield* decodeBatch(current.parts.join("")).pipe(
                Effect.mapError((cause) => new HubTransportError({ reason: "protocol", cause })),
              );
              yield* archiveBatch(frame.requestId, batch);
            }
            break;
          }
          default: {
            if ("channelId" in frame) {
              const queue = worker.channels.get(frame.channelId);
              if (queue)
                yield* Queue.offer(queue, frame).pipe(
                  Effect.timeout("10 seconds"),
                  Effect.catchTags({
                    TimeoutError: Effect.fn("HubTransport.closeSlowChannel")(function* () {
                      worker.channels.delete(frame.channelId);
                      yield* Queue.shutdown(queue);
                      yield* worker.send({ type: "close", channelId: frame.channelId });
                    }),
                  }),
                );
            }
          }
        }
      });
      const { pull } = yield* socket.reader;
      const receive = Effect.forever(
        Effect.gen(function* () {
          for (const frame of yield* pull) {
            lastReceivedAt = yield* Clock.currentTimeMillis;
            const text = typeof frame === "string" ? frame : new TextDecoder().decode(frame);
            if (text.length > 8_388_608)
              return yield* new HubTransportError({ reason: "protocol" });
            yield* handle(
              yield* decodeFrame(text).pipe(
                Effect.mapError((cause) => new HubTransportError({ reason: "protocol", cause })),
              ),
            );
          }
        }),
      );
      const heartbeat = Effect.forever(
        Effect.gen(function* () {
          yield* Effect.sleep("20 seconds");
          if ((yield* Clock.currentTimeMillis) - lastReceivedAt > 60_000)
            return yield* new HubTransportError({ reason: "offline" });
          yield* worker.send({ type: "ping" });
        }),
      );
      yield* Effect.raceFirst(Effect.raceFirst(receive, heartbeat), Deferred.await(closed));
    },
    Effect.mapError((cause) =>
      isHubTransportError(cause) ? cause : new HubTransportError({ reason: "protocol", cause }),
    ),
  );

  const proxy = Effect.fn("HubTransport.proxy")(
    function* (
      environmentId: EnvironmentId,
      path: string,
      scopes: ReadonlyArray<AuthEnvironmentScope>,
      request: HttpServerRequest.HttpServerRequest,
    ) {
      yield* workerFor(environmentId);
      const worker = active.get(environmentId);
      if (!worker) return yield* new HubTransportError({ reason: "offline" });
      const channelId = NodeCrypto.randomUUID();
      const queue = yield* Queue.bounded<HubWorkerFrame>(32);
      worker.channels.set(channelId, queue);
      const close = Effect.gen(function* () {
        worker.channels.delete(channelId);
        yield* Queue.shutdown(queue);
        yield* worker.send({ type: "close", channelId }).pipe(Effect.ignore);
      });
      yield* Effect.addFinalizer(() => close);
      if (request.headers.upgrade?.toLowerCase() === "websocket") {
        const client = yield* request.upgrade;
        const writer = yield* client.writer;
        yield* worker.send({ type: "ws.open", channelId, path, scopes });
        const { pull } = yield* client.reader;
        const toWorker = Effect.forever(
          Effect.gen(function* () {
            for (const data of yield* pull)
              yield* worker.send({
                type: "ws.data",
                channelId,
                data: typeof data === "string" ? data : Buffer.from(data).toString("base64"),
                binary: typeof data !== "string",
              });
          }),
        );
        const toClient = Effect.forever(
          Effect.gen(function* () {
            const frame = yield* Queue.take(queue);
            if (frame.type === "close") return yield* Effect.interrupt;
            if (frame.type === "ws.data")
              yield* writer.write(frame.binary ? Buffer.from(frame.data, "base64") : frame.data);
          }),
        );
        yield* Effect.raceFirst(toWorker, toClient).pipe(Effect.ignoreCause);
        return HttpServerResponse.empty();
      }
      const headers: Record<string, string> = {};
      const drop = new Set([
        "host",
        "cookie",
        "authorization",
        "dpop",
        "origin",
        "connection",
        "upgrade",
        "content-length",
        "accept-encoding",
        "x-forwarded-host",
        "x-forwarded-proto",
        "forwarded",
      ]);
      for (const [key, value] of Object.entries(request.headers))
        if (value !== undefined && !drop.has(key)) headers[key] = value;
      yield* worker.send({
        type: "http.open",
        channelId,
        method: request.method,
        path,
        headers,
        scopes,
      });
      yield* request.stream.pipe(
        Stream.runForEach((data) =>
          Effect.gen(function* () {
            for (let offset = 0; offset < data.length; offset += 131_072)
              yield* worker.send({
                type: "http.data",
                channelId,
                data: Buffer.from(data.subarray(offset, offset + 131_072)).toString("base64"),
              });
          }),
        ),
        Effect.andThen(worker.send({ type: "http.end", channelId })),
        Effect.forkScoped,
      );
      const first = yield* Queue.take(queue).pipe(
        Effect.timeout("30 seconds"),
        Effect.catchTags({
          TimeoutError: (cause) => Effect.fail(new HubTransportError({ reason: "timeout", cause })),
        }),
      );
      if (first.type !== "http.response") return yield* new HubTransportError({ reason: "worker" });
      const body = Stream.fromQueue(queue).pipe(
        Stream.takeWhile((frame) => frame.type !== "http.end" && frame.type !== "close"),
        Stream.filter((frame) => frame.type === "http.data"),
        Stream.map((frame) => Buffer.from(frame.data, "base64")),
        Stream.ensuring(close),
      );
      return HttpServerResponse.stream(body, {
        status: first.status,
        headers: { ...first.headers, "cache-control": "no-store, no-transform" },
      });
    },
    Effect.mapError((cause) =>
      isHubTransportError(cause) ? cause : new HubTransportError({ reason: "protocol", cause }),
    ),
  );
  const authorizeWorker = Effect.fn("HubTransport.authorizeWorker")(function* (
    workerId: string,
    token: string,
  ) {
    if (
      !(yield* read()).workers.some(
        (worker) => worker.workerId === workerId && matches(token, worker.tokenHash),
      )
    )
      return yield* new HubTransportError({ reason: "unauthorized" });
  });
  return HubTransport.of({
    list,
    createEnrollment,
    enroll,
    remove,
    descriptor,
    authorizeWorker,
    attach,
    requestWorker,
    proxy,
  });
});

export const layer = Layer.effect(HubTransport, make);
