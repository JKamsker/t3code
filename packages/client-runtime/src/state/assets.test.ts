import { describe, expect, it } from "@effect/vitest";
import { type AssetCreateUrlResult, EnvironmentId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import { createProjectFaviconUrlAtomFamily, resolveAssetUrl } from "./assets.ts";
import { authorizeHubSignedUrl } from "./hubMedia.ts";
import { HubConnectionTarget, type PreparedHttpAuthorization } from "../connection/model.ts";
import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import * as RpcHttp from "../rpc/http.ts";

describe("project favicon URL cache", () => {
  it.effect.each([
    { authorization: null, path: "/api/assets/worker-token/image.png?download=1" },
    {
      authorization: { _tag: "Bearer", token: "hub-token" },
      path: "/api/assets/worker-token/image.png?download=1",
    },
    {
      authorization: { _tag: "Dpop", accessToken: "old-token", expiresAtEpochMs: 0 },
      path: "/api/attachments/upload/worker-token",
    },
  ] satisfies ReadonlyArray<{ authorization: PreparedHttpAuthorization | null; path: string }>)(
    "authenticates signed worker URLs at the hub and caps their expiry",
    ({ authorization, path }) =>
      Effect.gen(function* () {
        const hubId = EnvironmentId.make("hub");
        const workerId = EnvironmentId.make("worker");
        const httpBaseUrl = "https://hub.test/api/hub/environments/worker/";
        const calls: Array<{ url: string; headers: Headers }> = [];
        const prepared = {
          environmentId: workerId,
          label: "Worker",
          httpBaseUrl,
          socketUrl: `${httpBaseUrl.replace(/^http/, "ws")}ws`,
          target: new HubConnectionTarget({
            environmentId: workerId,
            hubEnvironmentId: hubId,
            label: "Worker",
          }),
          httpAuthorization: authorization,
        };
        const httpLayer = RpcHttp.layerRemoteHttpClient(async (request, init) => {
          calls.push({ url: String(request), headers: new Headers(init?.headers) });
          return Response.json({ ticket: "hub-media", expiresAt: "2026-10-29T00:05:00Z" });
        });
        const asset = yield* authorizeHubSignedUrl(prepared, {
          relativeUrl: path,
          expiresAt: 4_000_000_000_000,
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              httpLayer,
              Layer.succeed(ManagedRelay.ManagedRelayDpopSigner, {
                thumbprint: Effect.succeed("key"),
                createProof: () => Effect.succeed("hub-proof"),
              }),
              Layer.succeed(RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization, {
                authorizeBearer: () => Effect.die("unused"),
                authorizeDpop: () => Effect.die("unused"),
                authorizeDpopHttp: ({ expectedEnvironmentId }) => {
                  expect(expectedEnvironmentId).toBe(hubId);
                  return Effect.succeed({
                    environmentId: hubId,
                    label: "Home server",
                    httpBaseUrl: "https://hub.test",
                    httpAuthorization: {
                      _tag: "Dpop",
                      accessToken: "renewed-token",
                      expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
                    },
                  });
                },
              }),
            ),
          ),
        );
        const url = new URL(asset.relativeUrl);
        expect(url.origin).toBe("https://hub.test");
        expect(url.pathname).toBe(`/api/hub/environments/worker${path.split("?")[0]}`);
        expect(url.searchParams.get("wsTicket")).toBe("hub-media");
        expect(url.searchParams.get("download")).toBe(path.includes("download") ? "1" : null);
        expect(asset.expiresAt).toBe(Date.parse("2026-10-29T00:05:00Z"));
        expect(calls).toHaveLength(1);
        expect(calls[0]!.url).toBe(`${httpBaseUrl}api/auth/websocket-ticket/media`);
        expect(calls[0]!.headers.get("authorization")).toBe(
          authorization === null
            ? null
            : authorization._tag === "Bearer"
              ? "Bearer hub-token"
              : "DPoP renewed-token",
        );
        const external = { relativeUrl: "https://cdn.test/image.png", expiresAt: 1 };
        expect(
          yield* authorizeHubSignedUrl(prepared, external).pipe(Effect.provide(httpLayer)),
        ).toBe(external);
        expect(calls).toHaveLength(1);
      }),
  );

  it("keeps worker assets under their home server route", () => {
    expect(
      resolveAssetUrl(
        "https://hub.test/api/hub/environments/worker/",
        "/api/assets/token/icon.svg",
      ),
    ).toBe("https://hub.test/api/hub/environments/worker/api/assets/token/icon.svg");
    expect(
      resolveAssetUrl(
        "https://hub.test/api/hub/environments/worker/",
        "https://cdn.test/image.png",
      ),
    ).toBe("https://cdn.test/image.png");
  });
  it("does not reuse another environment, workspace, or selected icon's cached URL", () => {
    const registry = AtomRegistry.make();
    const result = Atom.make<AsyncResult.AsyncResult<AssetCreateUrlResult, unknown>>(
      AsyncResult.success({
        expiresAt: 4_000_000_000_000,
        relativeUrl: "/api/assets/token/icon.svg",
      }),
    );
    const favicon = createProjectFaviconUrlAtomFamily({
      createUrl: () => result,
      preparedConnection: () => Atom.make(Option.some({ httpBaseUrl: "https://remote.test" })),
    });
    const target = { environmentId: EnvironmentId.make("remote"), cwd: "/workspace" };
    const unmount = registry.mount(favicon(target));
    try {
      expect(registry.get(favicon(target))).toBe("https://remote.test/api/assets/token/icon.svg");
      registry.set(result, AsyncResult.failure(Cause.die("disconnected")));
      expect(
        registry.get(favicon({ ...target, environmentId: EnvironmentId.make("other") })),
      ).toBeNull();
      expect(registry.get(favicon({ ...target, cwd: "/other" }))).toBeNull();
      expect(registry.get(favicon({ ...target, faviconPath: "brand.svg" }))).toBeNull();
      expect(registry.get(favicon({ ...target, faviconPath: null }))).toBe(
        "https://remote.test/api/assets/token/icon.svg",
      );
    } finally {
      unmount();
      registry.dispose();
    }
  });
});
