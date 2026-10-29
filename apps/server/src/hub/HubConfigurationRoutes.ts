import {
  AuthAccessReadScope,
  requiredScopesForHubConfigurationIntent,
  HUB_CONFIGURATION_MANAGEMENT_SCOPES,
  type HubConfigurationIntent,
  AuthAccessWriteScope,
  EnvironmentHttpApi,
  HubObservedConfiguration,
  HubConfigurationApply,
  EnvironmentHttpConflictError,
  EnvironmentHttpInternalServerError,
  EnvironmentId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as HubConfiguration from "./HubConfiguration.ts";
import * as HubTransport from "./HubTransport.ts";

const encodeApply = Schema.encodeEffect(Schema.toCodecJson(HubConfigurationApply));
const decodeObserved = Schema.decodeUnknownEffect(HubObservedConfiguration);
const mapError = (cause: HubConfiguration.HubConfigurationError) =>
  cause.reason === "conflict" || cause.reason === "request-conflict"
    ? new EnvironmentHttpConflictError({ message: cause.message })
    : new EnvironmentHttpInternalServerError({ message: cause.message });

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "hubConfiguration",
  Effect.fnUntraced(function* (handlers) {
    const config = yield* HubConfiguration.HubConfiguration;
    const transport = yield* HubTransport.HubTransport;
    const verify = Effect.fn("HubConfigurationRoutes.verify")(function* (id: EnvironmentId) {
      yield* transport.descriptor(id).pipe(
        Effect.mapError(
          () =>
            new EnvironmentHttpConflictError({
              message: "Machine is not enrolled with this hub.",
            }),
        ),
      );
    });
    const authorizeIntent = Effect.fn("HubConfigurationRoutes.authorizeIntent")(function* (
      intent: HubConfigurationIntent,
    ) {
      const scopes = requiredScopesForHubConfigurationIntent(intent);
      for (const scope of scopes) yield* requireEnvironmentScope(scope);
    });
    const authorizeManagement = Effect.forEach(
      HUB_CONFIGURATION_MANAGEMENT_SCOPES,
      (scope) => requireEnvironmentScope(scope),
      { discard: true },
    );
    const reconcile = Effect.fn("HubConfigurationRoutes.reconcile")(function* (
      environmentId: EnvironmentId,
    ) {
      yield* transport
        .requestWorker(environmentId, "hub.configuration.ready", { ready: false })
        .pipe(Effect.ignore);
      const state = yield* config.reconcile(environmentId, (input) =>
        encodeApply(input).pipe(
          Effect.flatMap((payload) =>
            transport.requestWorker(environmentId, "hub.configuration.apply", payload),
          ),
          Effect.asVoid,
        ),
      );
      yield* transport
        .requestWorker(environmentId, "hub.configuration.ready", {
          ready: !state.entries.some(
            (entry) => entry.status === "pending" || entry.status === "failed",
          ),
        })
        .pipe(Effect.ignore);
      yield* transport.requestWorker(environmentId, "hub.configuration.snapshot", null).pipe(
        Effect.flatMap(decodeObserved),
        Effect.flatMap((snapshot) => config.observe(environmentId, snapshot)),
        Effect.ignore,
      );
      return state;
    });
    return handlers
      .handle(
        "observedConfiguration",
        Effect.fn("hub.configuration.observed")(function* ({ params }) {
          yield* requireEnvironmentScope(AuthAccessReadScope);
          const environmentId = EnvironmentId.make(params.environmentId);
          yield* verify(environmentId);
          yield* transport.requestWorker(environmentId, "hub.configuration.snapshot", null).pipe(
            Effect.flatMap(decodeObserved),
            Effect.flatMap((snapshot) => config.observe(environmentId, snapshot)),
            Effect.ignore,
          );
          return yield* config.observed(environmentId).pipe(Effect.mapError(mapError));
        }),
      )
      .handle(
        "configuration",
        Effect.fn("hub.configuration.list")(function* ({ params }) {
          yield* requireEnvironmentScope(AuthAccessReadScope);
          yield* verify(EnvironmentId.make(params.environmentId));
          return yield* config
            .list(EnvironmentId.make(params.environmentId))
            .pipe(Effect.mapError(mapError));
        }),
      )
      .handle(
        "enqueueConfiguration",
        Effect.fn("hub.configuration.enqueue")(function* ({ params, payload }) {
          yield* requireEnvironmentScope(AuthAccessWriteScope);
          if (params.environmentId !== payload.environmentId)
            return yield* new EnvironmentHttpConflictError({
              message: "Machine identity does not match the configuration.",
            });
          yield* verify(payload.environmentId);
          yield* authorizeIntent(payload.intent);
          const state = yield* config.enqueue(payload).pipe(Effect.mapError(mapError));
          yield* reconcile(payload.environmentId).pipe(Effect.forkDetach);
          return state;
        }),
      )
      .handle(
        "cancelConfiguration",
        Effect.fn("hub.configuration.cancel")(function* ({ params, payload }) {
          yield* requireEnvironmentScope(AuthAccessWriteScope);
          yield* authorizeManagement;
          yield* verify(EnvironmentId.make(params.environmentId));
          const environmentId = EnvironmentId.make(params.environmentId);
          const state = yield* config
            .cancel(environmentId, payload.requestId)
            .pipe(Effect.mapError(mapError));
          yield* transport
            .requestWorker(environmentId, "hub.configuration.ready", {
              ready: !state.entries.some(
                (entry) => entry.status === "pending" || entry.status === "failed",
              ),
            })
            .pipe(Effect.ignore);
          return state;
        }),
      )
      .handle(
        "retryConfiguration",
        Effect.fn("hub.configuration.retry")(function* ({ params }) {
          yield* requireEnvironmentScope(AuthAccessWriteScope);
          yield* authorizeManagement;
          yield* verify(EnvironmentId.make(params.environmentId));
          return yield* reconcile(EnvironmentId.make(params.environmentId)).pipe(
            Effect.mapError(mapError),
          );
        }),
      );
  }),
);
