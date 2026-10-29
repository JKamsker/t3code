// @effect-diagnostics nodeBuiltinImport:off -- Real loopback servers verify forwarding boundaries.
import { NodeWS } from "@effect/platform-node/NodeSocket";
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentId,
  DEFAULT_SERVER_SETTINGS,
  HubObservedConfiguration,
  IsoDateTime,
  HubArchiveBatch,
  HubWorkerFrame,
  type ExecutionEnvironmentDescriptor,
  type HubConfigurationApply,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as NodeHttp from "node:http";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as HubArchiveExporter from "./HubArchiveExporter.ts";
import * as HubReconciler from "./HubReconciler.ts";
import * as HubSynchronization from "./HubSynchronization.ts";
import * as HubWorker from "./HubWorker.ts";
import * as HubWorkerConfig from "./HubWorkerConfig.ts";

const frameCodec = Schema.fromJsonString(Schema.toCodecJson(HubWorkerFrame));
const encodeFrame = Schema.encodeSync(frameCodec);
const decodeFrame = Schema.decodeUnknownSync(frameCodec);
const decodeArchiveBatch = Schema.decodeEffect(
  Schema.fromJsonString(Schema.toCodecJson(HubArchiveBatch)),
);
const decodeObserved = Schema.decodeUnknownEffect(HubObservedConfiguration);
const observed: HubObservedConfiguration = {
  settings: DEFAULT_SERVER_SETTINGS,
  projects: { projects: [], updatedAt: IsoDateTime.make("2026-01-01T00:00:00.000Z") },
  automation: { tasks: [] },
  hashes: { settings: "settings-hash" },
};
const descriptor: ExecutionEnvironmentDescriptor = {
  environmentId: EnvironmentId.make("worker-environment"),
  label: "Worker",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "1.0.0",
  capabilities: { repositoryIdentity: true },
};

const fixture = Effect.fn("HubWorker.testFixture")(function* (
  batches: ReadonlyArray<HubArchiveBatch> = [],
  archiveCursor = 0,
  archiveFailure?: HubArchiveExporter.HubArchiveExportError,
) {
  const frames = yield* Queue.unbounded<HubWorkerFrame>();
  const accepted = Promise.withResolvers<NodeWS.WebSocket>();
  const hub = yield* Effect.acquireRelease(
    Effect.sync(() => new NodeWS.WebSocketServer({ port: 0, host: "127.0.0.1" })),
    (server) =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            for (const client of server.clients) client.terminate();
            server.close(() => resolve());
          }),
      ),
  );
  if (!hub.address()) {
    yield* Effect.promise(() => new Promise<void>((resolve) => hub.once("listening", resolve)));
  }
  const hubAddress = hub.address();
  if (typeof hubAddress !== "object" || !hubAddress) throw new Error("Missing hub address");
  let hubAuthorization: string | undefined;
  hub.on("connection", (socket, request) => {
    hubAuthorization = request.headers.authorization;
    socket.on("message", (data) => {
      const frame = decodeFrame(data.toString());
      Queue.offerUnsafe(frames, frame);
      if (frame.type === "request" && frame.method === "hub.archive.status") {
        socket.send(
          encodeFrame({
            type: "response",
            requestId: frame.requestId,
            success: true,
            payload: {
              storeId: archiveCursor > 0 ? "store" : null,
              ackSequence: archiveCursor,
              anchorEventId: archiveCursor > 0 ? "event-anchor" : null,
              syncError: null,
            },
          }),
        );
      }
    });
    accepted.resolve(socket);
  });

  const requests = yield* Queue.unbounded<{
    path: string;
    headers: NodeHttp.IncomingHttpHeaders;
    body: string;
  }>();
  const localSockets = yield* Effect.acquireRelease(
    Effect.sync(() => new NodeWS.WebSocketServer({ noServer: true })),
    (server) =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            for (const client of server.clients) client.terminate();
            server.close(() => resolve());
          }),
      ),
  );
  const local = yield* Effect.acquireRelease(
    Effect.promise(
      () =>
        new Promise<NodeHttp.Server>((resolve) => {
          const server = NodeHttp.createServer((request, response) => {
            const chunks: Buffer[] = [];
            request.on("data", (chunk: Buffer) => chunks.push(chunk));
            request.on("end", () => {
              const body = Buffer.concat(chunks).toString();
              Queue.offerUnsafe(requests, { path: request.url!, headers: request.headers, body });
              response.writeHead(201, {
                "content-type": "text/plain",
                "set-cookie": "local-secret=1",
              });
              response.end(`echo:${body}`);
            });
          });
          server.on("upgrade", (request, socket, head) => {
            Queue.offerUnsafe(requests, { path: request.url!, headers: request.headers, body: "" });
            localSockets.handleUpgrade(request, socket, head, (ws) => {
              ws.on("message", (data, binary) => ws.send(data, { binary }));
              ws.send("ready");
            });
          });
          server.listen(0, "127.0.0.1", () => resolve(server));
        }),
    ),
    (server) =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            server.closeAllConnections();
            server.close(() => resolve());
          }),
      ),
  );
  const localAddress = local.address();
  if (typeof localAddress !== "object" || !localAddress) throw new Error("Missing local address");
  const issued: Parameters<SessionStore.SessionStore["Service"]["issue"]>[0][] = [];
  const revoked: string[] = [];
  const applied: HubConfigurationApply[] = [];
  const exportedCursors: number[] = [];
  const exportedAnchors: Array<string | null | undefined> = [];
  const ready = yield* Ref.make(true);
  const worker = yield* HubWorker.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeHttpClient.layerUndici,
        Layer.succeed(HubWorkerConfig.HubWorkerConfig, {
          status: Effect.succeed({
            hubUrl: `http://127.0.0.1:${hubAddress.port}`,
            hubId: EnvironmentId.make("hub"),
            workerId: "worker",
            workerToken: "worker-secret",
          }),
          join: () => Effect.die("Unexpected enrollment"),
          leave: Effect.void,
        }),
        Layer.succeed(ServerEnvironment.ServerEnvironment, {
          getEnvironmentId: Effect.succeed(descriptor.environmentId),
          getDescriptor: Effect.succeed(descriptor),
        }),
        Layer.succeed(SessionStore.SessionStore, {
          issue: (input: Parameters<SessionStore.SessionStore["Service"]["issue"]>[0]) =>
            Effect.sync(() => {
              issued.push(input);
              return {
                sessionId: AuthSessionId.make(`session-${issued.length}`),
                token: "local-secret",
                method: "bearer-access-token",
                client: { deviceType: "unknown" },
                expiresAt: DateTime.makeUnsafe(300_000),
                scopes: input?.scopes ?? [],
              };
            }),
          revoke: (id: AuthSessionId) =>
            Effect.sync(() => {
              revoked.push(id);
              return true;
            }),
        } as unknown as SessionStore.SessionStore["Service"]),
        Layer.succeed(HubArchiveExporter.HubArchiveExporter, {
          storeId: "store",
          stream: ({ afterSequence, afterEventId }) => {
            exportedCursors.push(afterSequence ?? 0);
            exportedAnchors.push(afterEventId);
            return archiveFailure ? Stream.fail(archiveFailure) : Stream.fromIterable(batches);
          },
        }),
        Layer.succeed(HubReconciler.HubReconciler, {
          snapshot: Effect.succeed(observed),
          apply: (input) =>
            Effect.sync(() => {
              applied.push(input);
            }),
        }),
        Layer.succeed(HubSynchronization.HubSynchronization, {
          ready: Ref.get(ready),
          pause: Ref.set(ready, false),
          resume: Ref.set(ready, true),
        }),
      ),
    ),
    Layer.build,
    Effect.map((context) => Context.get(context, HubWorker.HubWorker)),
  );
  yield* worker.start(`http://127.0.0.1:${localAddress.port}`);
  const socket = yield* Effect.promise(() => accepted.promise);
  const next = Effect.fn("HubWorker.nextTestFrame")(function* (
    predicate: (frame: HubWorkerFrame) => boolean,
  ) {
    while (true) {
      const frame = yield* Queue.take(frames);
      if (predicate(frame)) return frame;
    }
  });
  yield* next((frame) => frame.type === "hello");
  return {
    send: (frame: HubWorkerFrame) => Effect.sync(() => socket.send(encodeFrame(frame))),
    next,
    requests,
    issued,
    revoked,
    applied,
    ready,
    hubAuthorization: () => hubAuthorization,
    exportedCursors,
    exportedAnchors,
  };
});

it.live("streams authenticated local HTTP and binary WebSockets while rejecting hub loops", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    expect(test.hubAuthorization()).toBe("Bearer worker-secret");
    yield* test.send({
      type: "http.open",
      channelId: "http",
      method: "POST",
      path: "/api/echo?wsTicket=stolen&token=stolen&keep=yes",
      headers: {
        Authorization: "Bearer stolen",
        Cookie: "stolen=1",
        DPoP: "stolen",
        Origin: "https://attacker.invalid",
        "X-Forwarded-Host": "attacker.invalid",
        "Content-Type": "text/plain",
      },
      scopes: [AuthOrchestrationReadScope, "review:write"],
    });
    yield* test.send({
      type: "http.data",
      channelId: "http",
      data: Buffer.from("request-body").toString("base64"),
    });
    yield* test.send({ type: "http.end", channelId: "http" });
    const received = yield* Queue.take(test.requests);
    expect(received).toMatchObject({
      path: "/api/echo?keep=yes",
      body: "request-body",
      headers: { authorization: "Bearer local-secret", "content-type": "text/plain" },
    });
    expect(received.headers.cookie).toBeUndefined();
    expect(received.headers.dpop).toBeUndefined();
    expect(received.headers.origin).toBeUndefined();
    expect(received.headers["x-forwarded-host"]).toBeUndefined();
    const response = yield* test.next((frame) => frame.type === "http.response");
    expect(response).toMatchObject({ status: 201 });
    if (response.type !== "http.response") throw new Error("Missing response");
    expect(response.headers["set-cookie"]).toBeUndefined();
    const data = yield* test.next((frame) => frame.type === "http.data");
    if (data.type !== "http.data") throw new Error("Missing body");
    expect(Buffer.from(data.data, "base64").toString()).toBe("echo:request-body");
    yield* test.next((frame) => frame.type === "close" && frame.channelId === "http");
    expect(test.issued[0]).toMatchObject({
      scopes: [AuthOrchestrationReadScope],
      subject: "hub:hub:worker",
      method: "bearer-access-token",
    });
    expect(test.revoked).toEqual(["session-1"]);

    yield* test.send({
      type: "ws.open",
      channelId: "ws",
      path: "/ws?wsTicket=stolen",
      scopes: [AuthOrchestrationReadScope],
    });
    expect((yield* Queue.take(test.requests)).headers.authorization).toBe("Bearer local-secret");
    yield* test.next((frame) => frame.type === "ws.data" && frame.data === "ready");
    yield* test.send({
      type: "ws.data",
      channelId: "ws",
      binary: true,
      data: Buffer.from([0, 128, 255]).toString("base64"),
    });
    expect(yield* test.next((frame) => frame.type === "ws.data")).toMatchObject({
      binary: true,
      data: "AID/",
    });
    yield* test.send({ type: "close", channelId: "ws" });

    yield* test.send({
      type: "http.open",
      channelId: "loop",
      method: "GET",
      path: "/api/%68ub/workers",
      headers: {},
      scopes: [],
    });
    yield* test.next((frame) => frame.type === "close" && frame.channelId === "loop");
    expect(test.issued).toHaveLength(2);
  }),
);

it.live(
  "acknowledges archive batches and namespaces configuration before releasing scheduled work",
  () =>
    Effect.gen(function* () {
      const batch: HubArchiveBatch = {
        storeId: "store",
        afterSequence: 7,
        throughSequence: 7,
        events: [],
        bootstrap: true,
      };
      const test = yield* fixture([batch, { ...batch, complete: true }], 7);
      expect(yield* Ref.get(test.ready)).toBe(false);
      const chunk = yield* test.next((frame) => frame.type === "archive.chunk");
      if (chunk.type !== "archive.chunk") throw new Error("Missing archive chunk");
      expect(test.exportedCursors).toEqual([7]);
      expect(test.exportedAnchors).toEqual(["event-anchor"]);
      expect(yield* decodeArchiveBatch(chunk.data)).toEqual(batch);
      yield* test.send({
        type: "archive.ack",
        requestId: chunk.requestId,
        ack: { storeId: "store", ackSequence: 8 },
      });
      const complete = yield* test.next((frame) => frame.type === "archive.chunk");
      expect(complete).toMatchObject({ requestId: "archive-2" });
      yield* test.send({
        type: "archive.ack",
        requestId: "archive-2",
        ack: { storeId: "store", ackSequence: 8 },
      });
      yield* test.send({
        type: "request",
        requestId: "snapshot",
        method: "hub.configuration.snapshot",
        payload: null,
      });
      const snapshot = yield* test.next(
        (frame) => frame.type === "response" && frame.requestId === "snapshot",
      );
      expect(snapshot).toMatchObject({ success: true });
      if (snapshot.type !== "response") throw new Error("Missing snapshot response");
      expect(yield* decodeObserved(snapshot.payload)).toEqual(observed);
      yield* test.send({
        type: "request",
        requestId: "apply",
        method: "hub.configuration.apply",
        payload: { requestId: "durable", intent: { type: "settings.patch", patch: {} } },
      });
      expect(
        yield* test.next((frame) => frame.type === "response" && frame.requestId === "apply"),
      ).toMatchObject({ success: true });
      expect(test.applied).toMatchObject([{ requestId: "hub:worker:durable" }]);
      expect(yield* Ref.get(test.ready)).toBe(false);
      yield* test.send({
        type: "request",
        requestId: "ready",
        method: "hub.configuration.ready",
        payload: { ready: true },
      });
      yield* test.next((frame) => frame.type === "response" && frame.requestId === "ready");
      expect(yield* Ref.get(test.ready)).toBe(true);
      yield* test.send({
        type: "request",
        requestId: "unknown",
        method: "unsupported",
        payload: null,
      });
      expect(
        yield* test.next((frame) => frame.type === "response" && frame.requestId === "unknown"),
      ).toMatchObject({ success: false, payload: { errorCode: "invalid" } });
    }),
);

it.live("reports archive divergence while continuing HTTP and configuration requests", () =>
  Effect.gen(function* () {
    const test = yield* fixture(
      [],
      7,
      new HubArchiveExporter.HubArchiveExportError({
        reason: "source-changed",
        cause: new Error("Restored source has a different event at the cursor"),
      }),
    );
    expect(
      yield* test.next((frame) => frame.type === "request" && frame.method === "hub.archive.error"),
    ).toMatchObject({ payload: { reason: "source-changed" } });
    yield* test.send({
      type: "http.open",
      channelId: "after-error",
      method: "GET",
      path: "/api/echo",
      headers: {},
      scopes: [AuthOrchestrationReadScope],
    });
    yield* test.send({ type: "http.end", channelId: "after-error" });
    expect(yield* test.next((frame) => frame.type === "http.response")).toMatchObject({
      status: 201,
    });
    yield* test.send({
      type: "request",
      requestId: "after-error",
      method: "hub.configuration.snapshot",
      payload: null,
    });
    expect(
      yield* test.next((frame) => frame.type === "response" && frame.requestId === "after-error"),
    ).toMatchObject({ success: true });
  }),
);
