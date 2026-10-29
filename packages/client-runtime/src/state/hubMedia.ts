import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import { environmentEndpointUrl } from "../environment/endpoint.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "./environmentHttpAuth.ts";

export const authorizeHubSignedUrl = Effect.fn("clientRuntime.state.authorizeHubSignedUrl")(
  function* <A extends { readonly relativeUrl: string; readonly expiresAt: number }>(
    prepared: PreparedConnection,
    asset: A,
  ) {
    if (
      prepared.target._tag !== "HubConnectionTarget" ||
      !asset.relativeUrl.startsWith("/") ||
      asset.relativeUrl.startsWith("//")
    )
      return asset;
    const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
    const remoteAuthorization = yield* Effect.serviceOption(
      RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization,
    );
    const ticket = yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared,
      signer,
      remoteAuthorization,
      group: "auth",
      method: "POST",
      url: (urls) => urls.webSocketTicketFor({ params: { use: "media" } }),
      timeoutMs: 8_000,
      request: ({ client, headers }) =>
        client.webSocketTicketFor({ headers, params: { use: "media" } }),
    });
    const url = new URL(environmentEndpointUrl(prepared.httpBaseUrl, asset.relativeUrl));
    url.searchParams.set("wsTicket", ticket.ticket);
    return {
      ...asset,
      relativeUrl: url.toString(),
      expiresAt: Math.min(asset.expiresAt, DateTime.toEpochMillis(ticket.expiresAt)),
    };
  },
);
