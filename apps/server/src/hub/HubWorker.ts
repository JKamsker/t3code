import {
  AuthStandardClientScopes,
  HubArchiveBatch,
  HubConfigurationApply,
  HubObservedConfiguration,
  HubWorkerFrame,
  NonNegativeInt,
  type AuthEnvironmentScope,
  type HubArchiveAck,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest } from "effect/http";
import * as Socket from "effect/socket/Socket";

import * as SessionStore from "../auth/SessionStore.ts";
import { upstreamWebSocketConstructor } from "../device/DeviceHubProxy.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as HubArchiveExporter from "./HubArchiveExporter.ts";
import * as HubReconciler from "./HubReconciler.ts";
import * as HubSynchronization from "./HubSynchronization.ts";
import * as HubWorkerConfig from "./HubWorkerConfig.ts";

class HubWorkerError extends Schema.TaggedError<HubWorkerError>()("HubWorkerError", {
  reason: Schema.Literals([
    "origin",
    "path",
    "method",
    "frame",
    "heartbeat",
    "archive",
    "source-changed",
  ]),
}) {}

export class HubWorker extends Context.Service<
  HubWorker,
  { readonly start: (localOrigin: string) => Effect.Effect<void, never, Scope.Scope> }
>()("t3/hub/HubWorker") {}

const Method = Schema.Literals(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]);
const decodeMethod = Schema.decodeUnknownEffect(Method);
const decodeReady = Schema.decodeUnknownEffect(Schema.Struct({ ready: Schema.Boolean }));
const decodeArchiveStatus = Schema.decodeUnknownEffect(
  Schema.Struct({
    storeId: Schema.NullOr(Schema.String),
    ackSequence: NonNegativeInt,
    anchorEventId: Schema.NullOr(Schema.String),
    syncError: Schema.NullOr(Schema.String),
  }),
);
const frameCodec = Schema.fromJsonString(Schema.toCodecJson(HubWorkerFrame));
const decodeFrame = Schema.decodeEffect(frameCodec);
const encodeFrame = Schema.encodeEffect(frameCodec);
const decodeApply = Schema.decodeUnknownEffect(HubConfigurationApply);
const encodeObserved = Schema.encodeEffect(Schema.toCodecJson(HubObservedConfiguration));
const isReconciliationError = Schema.is(HubReconciler.HubReconciliationError);
const isArchiveExportError = Schema.is(HubArchiveExporter.HubArchiveExportError);
const isWorkerError = Schema.is(HubWorkerError);
const encodeBatch = Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(HubArchiveBatch)));
const standardScopes = new Set<AuthEnvironmentScope>(AuthStandardClientScopes);
const droppedRequestHeaders = new Set([
  "authorization",
  "cookie",
  "dpop",
  "host",
  "origin",
  "connection",
  "upgrade",
  "content-length",
  "transfer-encoding",
  "accept-encoding",
  "forwarded",
  "proxy-authorization",
]);
const droppedResponseHeaders = new Set([
  "connection",
  "transfer-encoding",
  "content-encoding",
  "content-length",
  "set-cookie",
]);
const retrySchedule = Schedule.exponential("1 second").pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, Duration.seconds(30))),
  ),
  Schedule.jittered,
);

type HttpChannel = {
  readonly kind: "http";
  readonly input: Queue.Queue<Uint8Array, Cause.Done>;
  fiber?: Fiber.Fiber<void, never>;
};
type WebSocketChannel = {
  readonly kind: "ws";
  readonly input: Queue.Queue<Uint8Array | string, Cause.Done>;
  fiber?: Fiber.Fiber<void, never>;
};
type Channel = HttpChannel | WebSocketChannel;

const localUrl = (origin: string, path: string) => {
  const pathname = decodeURIComponent(path.split(/[?#]/, 1)[0]!);
  if (
    !path.startsWith("/") ||
    path.startsWith("//") ||
    path.includes("#") ||
    /[\u0000-\u0020\u007f]/.test(path) ||
    /[\u0000-\u001f\u007f]/.test(pathname) ||
    pathname.includes("\\") ||
    pathname.includes("//") ||
    pathname.split("/").some((part) => part === "." || part === "..") ||
    pathname === "/api/hub" ||
    pathname.startsWith("/api/hub/") ||
    !(
      pathname.startsWith("/api/") ||
      pathname === "/ws" ||
      pathname === "/.well-known/t3/environment" ||
      pathname === "/mcp" ||
      pathname.startsWith("/mcp/") ||
      pathname === "/oauth" ||
      pathname.startsWith("/oauth/")
    )
  )
    throw new Error("Invalid worker channel path");
  const url = new URL(path, origin);
  if (url.origin !== origin) throw new Error("Invalid worker channel origin");
  for (const name of url.searchParams.keys().toArray()) {
    if (/^(?:wsTicket|ticket|token|access_token|authToken|bootstrapToken)$/i.test(name)) {
      url.searchParams.delete(name);
    }
  }
  return url;
};

const sameLink = (
  left: HubWorkerConfig.HubWorkerLink | null,
  right: HubWorkerConfig.HubWorkerLink | null,
) =>
  left?.hubUrl === right?.hubUrl &&
  left?.hubId === right?.hubId &&
  left?.workerId === right?.workerId &&
  left?.workerToken === right?.workerToken;

const make = Effect.gen(function* () {
  const config = yield* HubWorkerConfig.HubWorkerConfig;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const sessions = yield* SessionStore.SessionStore;
  const reconciler = yield* HubReconciler.HubReconciler;
  const synchronization = yield* HubSynchronization.HubSynchronization;
  const archive = yield* HubArchiveExporter.HubArchiveExporter;
  const http = HttpClient.withScope(yield* HttpClient.HttpClient);

  const connect = Effect.fn("HubWorker.connect")(function* (
    origin: string,
    link: HubWorkerConfig.HubWorkerLink,
  ) {
    yield* synchronization.pause;
    const connectionScope = yield* Effect.scope;
    const endpoint = new URL(
      `/api/hub/workers/${encodeURIComponent(link.workerId)}/ws`,
      link.hubUrl,
    );
    endpoint.protocol = endpoint.protocol === "https:" ? "wss:" : "ws:";
    const socket = yield* Socket.makeWebSocket(endpoint.href, {
      headers: { authorization: `Bearer ${link.workerToken}` },
      openTimeout: "10 seconds",
    }).pipe(Effect.provide(upstreamWebSocketConstructor));
    const { pull } = yield* socket.reader;
    const writer = yield* socket.writer;
    const channels = new Map<string, Channel>();
    const acknowledgements = new Map<
      string,
      {
        readonly batch: HubArchiveBatch;
        readonly ready: Deferred.Deferred<HubArchiveAck>;
      }
    >();
    const archiveStatus = yield* Deferred.make<unknown, HubWorkerError>();
    let awaitingPong = false;

    // Frame serialization and pumping share the connection span and run for every data chunk.
    const send = Effect.fnUntraced(function* (frame: HubWorkerFrame) {
      yield* writer.write(yield* encodeFrame(frame));
    });
    const closeChannel = Effect.fnUntraced(function* (channelId: string) {
      const channel = channels.get(channelId);
      if (!channel) return;
      channels.delete(channelId);
      yield* Queue.shutdown(channel.input);
      if (channel.fiber) yield* Fiber.interrupt(channel.fiber);
    });
    const issueSession = Effect.fn("HubWorker.issueSession")(function* (
      scopes: ReadonlyArray<AuthEnvironmentScope>,
    ) {
      return yield* Effect.acquireRelease(
        sessions.issue({
          scopes: scopes.filter((scope) => standardScopes.has(scope)),
          subject: `hub:${link.hubId}:${link.workerId}`,
          method: "bearer-access-token",
          ttl: Duration.minutes(5),
        }),
        (session) => sessions.revoke(session.sessionId).pipe(Effect.ignore),
      );
    });
    const forwardHttp = Effect.fn("HubWorker.forwardHttp")(function* (
      frame: Extract<HubWorkerFrame, { type: "http.open" }>,
      channel: HttpChannel,
    ) {
      const url = yield* Effect.try({
        try: () => localUrl(origin, frame.path),
        catch: () => new HubWorkerError({ reason: "path" }),
      });
      const method = yield* decodeMethod(frame.method);
      const session = yield* issueSession(frame.scopes);
      const headers: Record<string, string> = {};
      for (const [name, value] of Object.entries(frame.headers)) {
        const lower = name.toLowerCase();
        if (
          droppedRequestHeaders.has(lower) ||
          lower.startsWith("x-forwarded-") ||
          lower.startsWith("sec-websocket-")
        )
          continue;
        headers[lower] = value;
      }
      headers.authorization = `Bearer ${session.token}`;
      const request = HttpClientRequest.make(method)(url.href).pipe(
        HttpClientRequest.setHeaders(headers),
        method === "GET" || method === "HEAD"
          ? (self) => self
          : HttpClientRequest.bodyStream(Stream.fromQueue(channel.input), {
              contentType: headers["content-type"],
            }),
      );
      const response = yield* http.execute(request);
      const responseHeaders: Record<string, string> = {};
      for (const [name, value] of Object.entries(response.headers)) {
        if (value !== undefined && !droppedResponseHeaders.has(name.toLowerCase()))
          responseHeaders[name] = value;
      }
      yield* send({
        type: "http.response",
        channelId: frame.channelId,
        status: response.status,
        headers: responseHeaders,
      });
      yield* Stream.runForEach(response.stream, (chunk) =>
        send({
          type: "http.data",
          channelId: frame.channelId,
          data: Buffer.from(chunk).toString("base64"),
        }),
      );
      yield* send({ type: "http.end", channelId: frame.channelId });
    });
    const forwardWebSocket = Effect.fn("HubWorker.forwardWebSocket")(function* (
      frame: Extract<HubWorkerFrame, { type: "ws.open" }>,
      channel: WebSocketChannel,
    ) {
      const url = yield* Effect.try({
        try: () => localUrl(origin, frame.path),
        catch: () => new HubWorkerError({ reason: "path" }),
      });
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      const session = yield* issueSession(frame.scopes);
      const local = yield* Socket.makeWebSocket(url.href, {
        headers: { authorization: `Bearer ${session.token}` },
        openTimeout: "10 seconds",
      }).pipe(Effect.provide(upstreamWebSocketConstructor));
      const reader = yield* local.reader;
      const sink = yield* local.writer;
      const receive = Effect.gen(function* () {
        while (true) {
          for (const data of yield* reader.pull) {
            yield* send({
              type: "ws.data",
              channelId: frame.channelId,
              binary: typeof data !== "string",
              data: typeof data === "string" ? data : Buffer.from(data).toString("base64"),
            });
          }
        }
      });
      yield* Effect.raceFirst(
        receive,
        Stream.runForEach(Stream.fromQueue(channel.input), sink.write),
      );
    });
    const openChannel = Effect.fn("HubWorker.openChannel")(function* (
      frame: Extract<HubWorkerFrame, { type: "http.open" | "ws.open" }>,
    ) {
      if (channels.has(frame.channelId) || channels.size >= 64) {
        yield* closeChannel(frame.channelId);
        yield* send({ type: "close", channelId: frame.channelId });
        return;
      }
      const channel: Channel =
        frame.type === "http.open"
          ? {
              kind: "http",
              input: yield* Queue.make<Uint8Array, Cause.Done>({
                capacity: 8,
                strategy: "dropping",
              }),
            }
          : {
              kind: "ws",
              input: yield* Queue.make<Uint8Array | string, Cause.Done>({
                capacity: 8,
                strategy: "dropping",
              }),
            };
      channels.set(frame.channelId, channel);
      const work =
        frame.type === "http.open" && channel.kind === "http"
          ? forwardHttp(frame, channel)
          : frame.type === "ws.open" && channel.kind === "ws"
            ? forwardWebSocket(frame, channel)
            : Effect.void;
      channel.fiber = yield* work.pipe(
        Effect.scoped,
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            if (channels.get(frame.channelId) === channel) channels.delete(frame.channelId);
            yield* Queue.shutdown(channel.input);
            if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)) return;
            yield* send({ type: "close", channelId: frame.channelId }).pipe(
              Effect.timeout("1 second"),
              Effect.ignore,
            );
          }),
        ),
        Effect.ignoreCause,
        Effect.forkIn(connectionScope),
      );
    });
    const applyRequest = Effect.fn("HubWorker.applyRequest")(function* (
      frame: Extract<HubWorkerFrame, { type: "request" }>,
    ) {
      if (frame.method === "hub.configuration.ready") {
        const payload = yield* decodeReady(frame.payload).pipe(Effect.result);
        if (payload._tag === "Success") {
          yield* payload.success.ready ? synchronization.resume : synchronization.pause;
        }
        yield* send({
          type: "response",
          requestId: frame.requestId,
          success: payload._tag === "Success",
          payload: payload._tag === "Success" ? null : { errorCode: "invalid" },
        });
        return;
      }
      if (frame.method === "hub.configuration.snapshot") {
        const snapshot = yield* reconciler.snapshot.pipe(
          Effect.flatMap(encodeObserved),
          Effect.result,
        );
        yield* send({
          type: "response",
          requestId: frame.requestId,
          success: snapshot._tag === "Success",
          payload: snapshot._tag === "Success" ? snapshot.success : { errorCode: "apply" },
        });
        return;
      }
      if (frame.method !== "hub.configuration.apply") {
        yield* send({
          type: "response",
          requestId: frame.requestId,
          success: false,
          payload: { errorCode: "invalid" },
        });
        return;
      }
      const result = yield* Effect.gen(function* () {
        yield* synchronization.pause;
        const input = yield* decodeApply(frame.payload);
        yield* reconciler.apply(
          {
            ...input,
            requestId: `${link.hubId}:${link.workerId}:${input.requestId}`,
          },
          `${link.hubId}:${link.workerId}`,
        );
      }).pipe(Effect.result);
      const errorCode =
        result._tag === "Failure" && isReconciliationError(result.failure)
          ? result.failure.reason
          : "invalid";
      yield* send({
        type: "response",
        requestId: frame.requestId,
        success: result._tag === "Success",
        payload: result._tag === "Success" ? null : { errorCode },
      });
    });
    const consume = Effect.gen(function* () {
      while (true) {
        for (const data of yield* pull) {
          const text = typeof data === "string" ? data : new TextDecoder().decode(data);
          if (text.length > 8 * 1024 * 1024) return yield* new HubWorkerError({ reason: "frame" });
          const frame = yield* decodeFrame(text);
          switch (frame.type) {
            case "ping":
              yield* send({ type: "pong" });
              break;
            case "pong":
              awaitingPong = false;
              break;
            case "http.open":
            case "ws.open":
              yield* openChannel(frame);
              break;
            case "http.data":
            case "ws.data": {
              const channel = channels.get(frame.channelId);
              if (!channel) break;
              if (frame.data.length > (frame.type === "ws.data" ? 6 * 1024 * 1024 : 512 * 1024)) {
                yield* closeChannel(frame.channelId);
                yield* send({ type: "close", channelId: frame.channelId });
                break;
              }
              const accepted =
                channel.kind === "http" && frame.type === "http.data"
                  ? yield* Queue.offer(channel.input, Buffer.from(frame.data, "base64"))
                  : channel.kind === "ws" && frame.type === "ws.data"
                    ? yield* Queue.offer(
                        channel.input,
                        frame.binary ? Buffer.from(frame.data, "base64") : frame.data,
                      )
                    : false;
              if (!accepted) {
                yield* closeChannel(frame.channelId);
                yield* send({ type: "close", channelId: frame.channelId });
              }
              break;
            }
            case "http.end": {
              const channel = channels.get(frame.channelId);
              if (channel?.kind === "http") yield* Queue.end(channel.input);
              break;
            }
            case "close":
              yield* closeChannel(frame.channelId);
              break;
            case "request":
              yield* applyRequest(frame).pipe(
                Effect.catchCauseIf(
                  (cause) => !Cause.hasInterrupts(cause),
                  () =>
                    Effect.logWarning("Hub worker could not answer a request.").pipe(
                      Effect.andThen(
                        send({
                          type: "response",
                          requestId: frame.requestId,
                          success: false,
                          payload: { errorCode: "apply" },
                        }),
                      ),
                    ),
                ),
                Effect.forkIn(connectionScope),
              );
              break;
            case "response":
              if (frame.requestId === "archive-status") {
                yield* frame.success
                  ? Deferred.succeed(archiveStatus, frame.payload)
                  : Deferred.fail(archiveStatus, new HubWorkerError({ reason: "archive" }));
              }
              break;
            case "archive.ack": {
              const pending = acknowledgements.get(frame.requestId);
              if (
                pending &&
                frame.ack.storeId === pending.batch.storeId &&
                frame.ack.ackSequence >= pending.batch.throughSequence
              ) {
                yield* Deferred.succeed(pending.ready, frame.ack);
              }
              break;
            }
            default:
              break;
          }
        }
      }
    });
    const heartbeat = Effect.gen(function* () {
      while (true) {
        yield* Effect.sleep("20 seconds");
        if (awaitingPong) return yield* new HubWorkerError({ reason: "heartbeat" });
        awaitingPong = true;
        yield* send({ type: "ping" });
      }
    });
    let batchId = 0;
    const replicate = Stream.unwrap(
      Effect.gen(function* () {
        yield* send({
          type: "request",
          requestId: "archive-status",
          method: "hub.archive.status",
          payload: { storeId: archive.storeId },
        });
        const status = yield* decodeArchiveStatus(
          yield* Deferred.await(archiveStatus).pipe(Effect.timeout("30 seconds")),
        );
        if (status.storeId !== null && status.storeId !== archive.storeId) {
          return yield* new HubWorkerError({ reason: "source-changed" });
        }
        return archive.stream({
          afterSequence: status.storeId === archive.storeId ? status.ackSequence : 0,
          afterEventId: status.storeId === archive.storeId ? status.anchorEventId : null,
        });
      }),
    ).pipe(
      Stream.runForEach(
        Effect.fn("HubWorker.sendArchiveBatch")(function* (batch) {
          const requestId = `archive-${++batchId}`;
          const ready = yield* Deferred.make<HubArchiveAck>();
          acknowledgements.set(requestId, { batch, ready });
          yield* Effect.gen(function* () {
            const data = yield* encodeBatch(batch);
            const total = Math.max(1, Math.ceil(data.length / (256 * 1024)));
            if (total > 1024 || data.length > 128 * 1024 * 1024) {
              return yield* new HubWorkerError({ reason: "archive" });
            }
            for (let index = 0; index < total; index++) {
              yield* send({
                type: "archive.chunk",
                requestId,
                index,
                total,
                data: data.slice(index * 256 * 1024, (index + 1) * 256 * 1024),
              });
            }
            yield* Deferred.await(ready).pipe(Effect.timeout("30 seconds"));
          }).pipe(Effect.ensuring(Effect.sync(() => acknowledgements.delete(requestId))));
        }),
      ),
      Effect.catch((error) => {
        if (Socket.isSocketError(error)) return Effect.fail(error);
        const reason =
          (isArchiveExportError(error) || isWorkerError(error)) && error.reason === "source-changed"
            ? "source-changed"
            : "storage";
        return send({
          type: "request",
          requestId: "archive-error",
          method: "hub.archive.error",
          payload: { reason },
        });
      }),
      Effect.andThen(Effect.never),
    );
    yield* send({ type: "hello", environment: yield* environment.getDescriptor });
    return yield* Effect.raceFirst(consume, Effect.raceFirst(heartbeat, replicate));
  });

  const supervise = Effect.fn("HubWorker.supervise")(function* (origin: string) {
    while (true) {
      const link = yield* config.status;
      if (!link) return;
      const changed = Effect.gen(function* () {
        while (true) {
          yield* Effect.sleep("30 seconds");
          if (!sameLink(link, yield* config.status)) return;
        }
      });
      yield* Effect.raceFirst(
        connect(origin, link).pipe(
          Effect.scoped,
          Effect.ensuring(synchronization.resume),
          Effect.retry({ schedule: retrySchedule }),
        ),
        changed,
      );
    }
  });
  const start = Effect.fn("HubWorker.start")(
    function* (localOrigin: string) {
      if (!(yield* config.status)) return;
      const origin = yield* Effect.try({
        try: () => {
          const url = new URL(localOrigin);
          if (
            (url.protocol !== "http:" && url.protocol !== "https:") ||
            !["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) ||
            url.username ||
            url.password ||
            url.pathname !== "/" ||
            url.search ||
            url.hash
          )
            throw new Error("Invalid local origin");
          return url.origin;
        },
        catch: () => new HubWorkerError({ reason: "origin" }),
      });
      yield* supervise(origin).pipe(Effect.retry({ schedule: retrySchedule }), Effect.forkScoped);
    },
    Effect.catch(() => Effect.logWarning("Hub worker could not start.")),
  );
  return HubWorker.of({ start });
});

export const layer = Layer.effect(HubWorker, make);
