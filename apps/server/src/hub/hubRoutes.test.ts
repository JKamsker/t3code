import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Layer from "effect/Layer";
import { HttpRouter, HttpServerResponse } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as HubTransport from "./HubTransport.ts";
import * as HubRoutes from "./hubRoutes.ts";

const layerAuth = EnvironmentAuth.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "hub-media-auth-test-" })),
  Layer.provideMerge(NodeServices.layer),
);

it.effect("authenticates routed media tickets without granting HTTP credential access", () =>
  Effect.gen(function* () {
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const sessions = yield* SessionStore.SessionStore;
    const clock = yield* Clock.Clock;
    const forwarded: { path: string; scopes: ReadonlyArray<AuthEnvironmentScope> }[] = [];
    const routes = HubRoutes.layer.pipe(
      Layer.provideMerge(
        Layer.mock(HubTransport.HubTransport)({
          proxy: (_id, path, scopes) =>
            Effect.sync(() => {
              forwarded.push({ path, scopes });
              return HttpServerResponse.text("media");
            }),
        }),
      ),
      Layer.provideMerge(Layer.succeed(EnvironmentAuth.EnvironmentAuth, auth)),
      Layer.provideMerge(Layer.succeed(SessionStore.SessionStore, sessions)),
      Layer.provideMerge(Layer.succeed(Clock.Clock, clock)),
    );
    const session = yield* auth.issueSession({ scopes: [AuthOrchestrationReadScope] });
    const media = yield* auth.issueWebSocketTicket(session, { use: "media" });
    const rpc = yield* auth.issueWebSocketTicket(session, { use: "rpc" });
    const restrictedSession = yield* auth.issueSession({ scopes: ["preview:operate"] });
    const restricted = yield* auth.issueWebSocketTicket(restrictedSession, { use: "media" });
    const writerSession = yield* auth.issueSession({ scopes: [AuthOrchestrationOperateScope] });
    const writer = yield* auth.issueWebSocketTicket(writerSession, { use: "media" });
    yield* Effect.acquireUseRelease(
      Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
      ({ handler }) =>
        Effect.gen(function* () {
          const request = (path: string, ticket?: string, method = "GET") =>
            Effect.promise(() =>
              handler(
                new Request(
                  `http://localhost/api/hub/environments/worker${path}${path.includes("?") ? "&" : "?"}${ticket ? `wsTicket=${encodeURIComponent(ticket)}` : ""}`,
                  { method },
                ),
              ),
            );
          for (const path of [
            "/api/device-hub/api/devices",
            "/api/preview-stream/download?id=download",
            "/api/assets/worker-signed-token/image.png",
          ]) {
            expect((yield* request(path, media.ticket)).status).toBe(200);
          }
          expect(forwarded).toEqual([
            { path: "/api/device-hub/api/devices", scopes: [AuthOrchestrationReadScope] },
            {
              path: "/api/preview-stream/download?id=download",
              scopes: [AuthOrchestrationReadScope],
            },
            {
              path: "/api/assets/worker-signed-token/image.png",
              scopes: [AuthOrchestrationReadScope],
            },
          ]);
          expect((yield* request("/api/assets/token/image.png", rpc.ticket)).status).toBe(401);
          expect((yield* request("/api/assets/token/image.png")).status).toBe(401);
          expect((yield* request("/api/auth/session", media.ticket)).status).toBe(401);
          expect((yield* request("/api/settings", media.ticket, "POST")).status).toBe(401);
          expect((yield* request("/api/assets/token/image.png", restricted.ticket)).status).toBe(
            403,
          );
          expect(forwarded).toHaveLength(3);
          expect(
            (yield* request("/api/attachments/upload/worker-signed-token", media.ticket, "POST"))
              .status,
          ).toBe(403);
          expect(
            (yield* request("/api/attachments/upload/worker-signed-token", writer.ticket, "POST"))
              .status,
          ).toBe(200);
          expect(forwarded[3]).toEqual({
            path: "/api/attachments/upload/worker-signed-token",
            scopes: [AuthOrchestrationOperateScope],
          });
          yield* auth.revokeSession(session.sessionId);
          expect((yield* request("/api/assets/token/image.png", media.ticket)).status).toBe(401);
        }),
      ({ dispose }) => Effect.promise(dispose),
    );
  }).pipe(Effect.provide(layerAuth)),
);
