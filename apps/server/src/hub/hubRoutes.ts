import {
  AuthAccessWriteScope,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentHttpApi,
  EnvironmentId,
  HubEnrollRequest,
  authScopeResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import { failEnvironmentInternal, requireEnvironmentScope } from "../auth/http.ts";
import * as HubTransport from "./HubTransport.ts";

const noStore = { "cache-control": "no-store" };
const transportFailure = (error: HubTransport.HubTransportError) =>
  HttpServerResponse.json(
    { reason: error.reason, message: error.message },
    {
      status:
        error.reason === "unauthorized"
          ? 401
          : error.reason === "enrollment"
            ? 400
            : error.reason === "missing"
              ? 404
              : error.reason === "offline" || error.reason === "timeout"
                ? 503
                : 500,
      headers: noStore,
    },
  );

export const layerApi = HttpApiBuilder.group(EnvironmentHttpApi, "hub", (handlers) =>
  Effect.gen(function* () {
    const hub = yield* HubTransport.HubTransport;
    return handlers
      .handle("environments", () =>
        requireEnvironmentScope(AuthOrchestrationReadScope).pipe(
          Effect.andThen(hub.list),
          Effect.map((environments) => ({ environments })),
          Effect.catchTags({
            HubTransportError: (cause) => failEnvironmentInternal("internal_error", cause),
          }),
        ),
      )
      .handle("createEnrollment", ({ payload }) =>
        requireEnvironmentScope(AuthAccessWriteScope).pipe(
          Effect.andThen(hub.createEnrollment(payload)),
          Effect.catchTags({
            HubTransportError: (cause) => failEnvironmentInternal("internal_error", cause),
          }),
        ),
      )
      .handle("removeEnvironment", ({ params }) =>
        requireEnvironmentScope(AuthAccessWriteScope).pipe(
          Effect.andThen(hub.remove(EnvironmentId.make(params.environmentId))),
          Effect.map((removed) => ({ removed })),
          Effect.catchTags({
            HubTransportError: (cause) => failEnvironmentInternal("internal_error", cause),
          }),
        ),
      );
  }),
);

const enrollRoute = HttpRouter.add(
  "POST",
  "/api/hub/enroll",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const hub = yield* HubTransport.HubTransport;
    const input = yield* request.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(HubEnrollRequest)),
    );
    return yield* hub.enroll(input).pipe(
      Effect.flatMap((result) => HttpServerResponse.json(result, { headers: noStore })),
      Effect.catchTags({ HubTransportError: transportFailure }),
    );
  }).pipe(
    Effect.orElseSucceed(() =>
      HttpServerResponse.text("Bad request", { status: 400, headers: noStore }),
    ),
  ),
);

const workerRoute = HttpRouter.add(
  "GET",
  "/api/hub/workers/:workerId/ws",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const hub = yield* HubTransport.HubTransport;
    const pathname = new URL(request.url, "http://hub.invalid").pathname;
    const workerId = decodeURIComponent(pathname.split("/")[4] ?? "");
    const authorization = request.headers.authorization ?? "";
    if (!authorization.startsWith("Bearer "))
      return HttpServerResponse.text("Unauthorized", { status: 401 });
    const token = authorization.slice(7);
    yield* hub.authorizeWorker(workerId, token);
    const socket = yield* request.upgrade;
    yield* hub.attach(workerId, token, socket).pipe(Effect.ignoreCause);
    return HttpServerResponse.empty();
  }).pipe(Effect.catchTags({ HubTransportError: transportFailure })),
);

const proxyRoute = HttpRouter.add(
  "*",
  "/api/hub/environments/:environmentId/*",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const hub = yield* HubTransport.HubTransport;
    const url = new URL(request.url, "http://hub.invalid");
    const match = /^\/api\/hub\/environments\/([^/]+)(\/.*)$/.exec(url.pathname);
    if (!match) return HttpServerResponse.text("Not found", { status: 404 });
    const environmentId = EnvironmentId.make(decodeURIComponent(match[1]!));
    const path = match[2]!;
    if (path === "/.well-known/t3/environment")
      return yield* hub
        .descriptor(environmentId)
        .pipe(
          Effect.flatMap((descriptor) => HttpServerResponse.json(descriptor, { headers: noStore })),
        );
    const websocket = request.headers.upgrade?.toLowerCase() === "websocket";
    const asset =
      (request.method === "GET" || request.method === "HEAD") && path.startsWith("/api/assets/");
    const attachmentUpload =
      request.method === "POST" && path.startsWith("/api/attachments/upload/");
    const media =
      asset ||
      attachmentUpload ||
      path.startsWith("/api/device-hub/") ||
      path.startsWith("/api/preview-stream/");
    const session = yield* (
      websocket || media
        ? auth.authenticateWebSocketUpgrade(request, path === "/ws" ? "rpc" : "media")
        : auth.authenticateHttpRequest(request)
    ).pipe(Effect.mapError(() => new HubTransport.HubTransportError({ reason: "unauthorized" })));
    if (asset && !session.scopes.includes(AuthOrchestrationReadScope))
      return HttpServerResponse.text("Insufficient scope", { status: 403, headers: noStore });
    if (attachmentUpload && !session.scopes.includes(AuthOrchestrationOperateScope))
      return HttpServerResponse.text("Insufficient scope", { status: 403, headers: noStore });
    if (path === "/api/auth/session")
      return yield* HttpServerResponse.json(
        {
          authenticated: true,
          auth: yield* auth.getDescriptor(),
          ...authScopeResponse(session.scopes),
          sessionMethod: session.method,
        },
        { headers: noStore },
      );
    if (
      request.method === "POST" &&
      /^\/api\/auth\/websocket-ticket(?:\/(rpc|media))?$/.test(path)
    ) {
      const use = path.endsWith("/rpc") ? "rpc" : path.endsWith("/media") ? "media" : undefined;
      const result = yield* auth.issueWebSocketTicket(
        session,
        use === undefined ? undefined : { use },
      );
      return yield* HttpServerResponse.schemaJson(
        Schema.toCodecJson(Schema.Struct({ ticket: Schema.String, expiresAt: Schema.DateTimeUtc })),
      )(result, { headers: noStore });
    }
    url.searchParams.delete("wsTicket");
    url.searchParams.delete("dpop");
    if (websocket) {
      const sessions = yield* SessionStore.SessionStore;
      return yield* Effect.acquireUseRelease(
        sessions.markConnected(session.sessionId),
        () =>
          Effect.raceFirst(
            hub.proxy(environmentId, `${path}${url.search}`, session.scopes, request),
            sessions
              .awaitInvalidation(session.sessionId)
              .pipe(Effect.as(HttpServerResponse.empty())),
          ),
        () => sessions.markDisconnected(session.sessionId),
      );
    }
    return yield* hub.proxy(environmentId, `${path}${url.search}`, session.scopes, request);
  }).pipe(
    Effect.catchTags({ HubTransportError: transportFailure }),
    Effect.orElseSucceed(() =>
      HttpServerResponse.text("Hub request failed", { status: 500, headers: noStore }),
    ),
  ),
);

export const layer = Layer.mergeAll(enrollRoute, workerRoute, proxyRoute);
