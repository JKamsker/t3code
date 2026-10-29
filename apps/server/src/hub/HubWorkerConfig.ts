import { HubEnrollResult, type HubEnrollRequest } from "@t3tools/contracts";
import {
  isLocalLoopbackHost,
  isPrivateNetworkHost,
  isSharedAddressSpaceHost,
  isTailnetHost,
} from "@t3tools/shared/hostClassification";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest } from "effect/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";

const Link = Schema.Struct({ ...HubEnrollResult.fields, hubUrl: Schema.String });
export type HubWorkerLink = typeof Link.Type;
const decodeLink = Schema.decodeUnknownEffect(Schema.fromJsonString(Link));
const encodeLink = Schema.encodeEffect(Schema.fromJsonString(Link));
const decodeEnrollResult = Schema.decodeUnknownEffect(HubEnrollResult);

export class HubWorkerConfigError extends Schema.TaggedError<HubWorkerConfigError>()(
  "HubWorkerConfigError",
  {
    reason: Schema.Literals(["url", "enrollment", "storage"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Hub worker configuration failed (${this.reason}).`;
  }
}

export class HubWorkerConfig extends Context.Service<
  HubWorkerConfig,
  {
    readonly status: Effect.Effect<HubWorkerLink | null, HubWorkerConfigError>;
    readonly join: (input: {
      readonly hubUrl: string;
      readonly code: string;
    }) => Effect.Effect<HubEnrollResult, HubWorkerConfigError>;
    readonly leave: Effect.Effect<void, HubWorkerConfigError>;
  }
>()("t3/hub/HubWorkerConfig") {}
const isHubWorkerConfigError = Schema.is(HubWorkerConfigError);

const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const http = yield* HttpClient.HttpClient;
  const status = Effect.gen(function* () {
    const saved = yield* secrets.get("hub-worker-link");
    return Option.isNone(saved) ? null : yield* decodeLink(new TextDecoder().decode(saved.value));
  }).pipe(Effect.mapError((cause) => new HubWorkerConfigError({ reason: "storage", cause })));
  const join = Effect.fn("HubWorkerConfig.join")(
    function* (input: { readonly hubUrl: string; readonly code: string }) {
      const hubUrl = yield* Effect.try({
        try: () => {
          const url = new URL(input.hubUrl);
          if (
            (url.protocol !== "https:" &&
              !(
                url.protocol === "http:" &&
                (isLocalLoopbackHost(url.hostname) ||
                  isPrivateNetworkHost(url.hostname) ||
                  isSharedAddressSpaceHost(url.hostname) ||
                  isTailnetHost(url.hostname))
              )) ||
            url.username ||
            url.password ||
            url.search ||
            url.hash ||
            url.pathname !== "/"
          )
            throw new Error("invalid hub origin");
          return url.origin;
        },
        catch: (cause) => new HubWorkerConfigError({ reason: "url", cause }),
      });
      const body: HubEnrollRequest = {
        code: input.code,
        environment: yield* environment.getDescriptor,
      };
      const request = yield* HttpClientRequest.bodyJson(
        HttpClientRequest.post(`${hubUrl}/api/hub/enroll`),
        body,
      );
      const response = yield* http.execute(request).pipe(Effect.timeout("30 seconds"));
      if (response.status !== 200) return yield* new HubWorkerConfigError({ reason: "enrollment" });
      const result = yield* decodeEnrollResult(yield* response.json);
      yield* secrets.set(
        "hub-worker-link",
        new TextEncoder().encode(yield* encodeLink({ ...result, hubUrl })),
      );
      return result;
    },
    Effect.mapError((cause) =>
      isHubWorkerConfigError(cause)
        ? cause
        : new HubWorkerConfigError({ reason: "enrollment", cause }),
    ),
  );
  return HubWorkerConfig.of({
    status,
    join,
    leave: secrets
      .remove("hub-worker-link")
      .pipe(Effect.mapError((cause) => new HubWorkerConfigError({ reason: "storage", cause }))),
  });
});

export const layer = Layer.effect(HubWorkerConfig, make);
