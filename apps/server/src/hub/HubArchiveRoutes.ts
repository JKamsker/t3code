import { AuthOrchestrationReadScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { failEnvironmentInternal, requireEnvironmentScope } from "../auth/http.ts";
import { assetResponseHeaders } from "../http.ts";
import * as HubArchive from "./HubArchive.ts";

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "hubArchive",
  Effect.fnUntraced(function* (handlers) {
    const archive = yield* HubArchive.HubArchive;
    const read = <A>(operation: Effect.Effect<A, HubArchive.HubArchiveError>) =>
      requireEnvironmentScope(AuthOrchestrationReadScope).pipe(
        Effect.andThen(operation),
        Effect.catchTags({
          HubArchiveError: (cause) => failEnvironmentInternal("internal_error", cause),
        }),
      );
    return handlers
      .handle("archiveStatus", ({ params }) => read(archive.status(params.environmentId)))
      .handle("archiveShell", ({ params, query }) =>
        read(archive.shell(params.environmentId, query.location)),
      )
      .handle("archiveThread", ({ params }) =>
        read(archive.thread(params.environmentId, params.threadId)),
      )
      .handle("archiveHistory", ({ params, query }) =>
        read(archive.history(params.environmentId, params.threadId, query.cursor)),
      )
      .handle("archiveTurnItem", ({ params }) =>
        read(archive.item(params.environmentId, params.threadId, params.itemId)),
      )
      .handleRaw("archiveAttachment", ({ params }) =>
        read(archive.asset(params.environmentId, params.attachmentId)).pipe(
          Effect.map((asset) =>
            asset === null
              ? HttpServerResponse.empty({ status: 404 })
              : HttpServerResponse.uint8Array(asset.bytes, {
                  contentType: asset.mimeType,
                  headers: assetResponseHeaders(
                    asset.mimeType.toLowerCase() === "image/svg+xml"
                      ? `${params.attachmentId}.svg`
                      : params.attachmentId,
                    { mimeType: asset.mimeType },
                  ),
                }),
          ),
        ),
      );
  }),
);
