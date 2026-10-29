import type { EnvironmentId, HubInventory } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../state/environmentHttpAuth.ts";
import { HubConnectionRegistration } from "./catalog.ts";
import * as HubConnections from "./hubConnections.ts";
import { HubConnectionTarget, type PreparedConnection } from "./model.ts";
import * as EnvironmentRegistry from "./registry.ts";
import * as EnvironmentSupervisor from "./supervisor.ts";
import { connectionRouteId, connectionRoutes } from "./routes.ts";

export const fetchHubInventory = Effect.fn("clientRuntime.connection.fetchHubInventory")(function* (
  prepared: PreparedConnection,
) {
  const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(
    RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization,
  );
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared,
    signer,
    remoteAuthorization,
    group: "hub",
    method: "GET",
    timeoutMs: 10_000,
    url: (urls) => urls.environments(),
    request: ({ client, headers }) => client.environments({ headers }),
  });
});

export const reconcileHubInventory = Effect.fn("clientRuntime.connection.reconcileHubInventory")(
  function* (hubEnvironmentId: EnvironmentId, inventory: HubInventory) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    for (const worker of inventory.environments) {
      if (worker.environmentId === hubEnvironmentId) continue;
      const target = new HubConnectionTarget({
        environmentId: worker.environmentId,
        label: worker.label,
        hubEnvironmentId,
      });
      const current = (yield* SubscriptionRef.get(registry.entries)).get(worker.environmentId);
      const existing =
        current === undefined
          ? undefined
          : connectionRoutes(current).find(
              (route) => connectionRouteId(route.target) === connectionRouteId(target),
            );
      if (existing === undefined || existing.target.label !== target.label) {
        yield* registry.register(new HubConnectionRegistration({ target }));
      }
    }
    const known = new Map(inventory.environments.map((worker) => [worker.environmentId, worker]));
    for (const [environmentId, entry] of yield* SubscriptionRef.get(registry.entries)) {
      if (known.has(environmentId)) continue;
      if (
        connectionRoutes(entry).some(
          (route) =>
            route.target._tag === "HubConnectionTarget" &&
            route.target.hubEnvironmentId === hubEnvironmentId,
        )
      ) {
        yield* registry.removeRoute(environmentId, `hub:${hubEnvironmentId}`);
      }
    }
    const reconnect: EnvironmentId[] = [];
    yield* SubscriptionRef.update(registry.entries, (entries) => {
      let updated = entries;
      for (const [environmentId, entry] of entries) {
        if (
          entry.target._tag !== "HubConnectionTarget" ||
          entry.target.hubEnvironmentId !== hubEnvironmentId
        )
          continue;
        const worker = known.get(environmentId);
        const sourceAvailability = {
          online: worker?.online ?? false,
          lastSeenAt: worker?.lastSeenAt ?? entry.sourceAvailability?.lastSeenAt ?? null,
        };
        if (
          entry.sourceAvailability?.online === sourceAvailability.online &&
          entry.sourceAvailability?.lastSeenAt === sourceAvailability.lastSeenAt
        )
          continue;
        if (sourceAvailability.online && entry.sourceAvailability?.online !== true)
          reconnect.push(environmentId);
        updated = new Map(updated).set(environmentId, { ...entry, sourceAvailability });
      }
      return updated;
    });
    yield* Effect.forEach(reconnect, registry.retryNow, { discard: true });
  },
);

export const markHubUnavailable = Effect.fn("clientRuntime.connection.markHubUnavailable")(
  function* (hubEnvironmentId: EnvironmentId) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const hubs = yield* HubConnections.HubConnections;
    yield* SubscriptionRef.update(hubs, (current) => {
      const next = new Map(current);
      next.delete(hubEnvironmentId);
      return next;
    });
    const disconnect: EnvironmentId[] = [];
    yield* SubscriptionRef.update(registry.entries, (entries) => {
      let updated = entries;
      for (const [environmentId, entry] of entries) {
        if (
          entry.target._tag === "HubConnectionTarget" &&
          entry.target.hubEnvironmentId === hubEnvironmentId &&
          entry.sourceAvailability?.online !== false
        ) {
          disconnect.push(environmentId);
          updated = new Map(updated).set(environmentId, {
            ...entry,
            sourceAvailability: {
              online: false,
              lastSeenAt: entry.sourceAvailability?.lastSeenAt ?? null,
            },
          });
        }
      }
      return updated;
    });
    yield* Effect.forEach(disconnect, registry.retryNow, { discard: true });
  },
);

export const watchHubInventories = Effect.fn("clientRuntime.connection.watchHubInventories")(
  function* () {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const hubs = yield* HubConnections.HubConnections;
    const watchers = new Map<EnvironmentId, Fiber.Fiber<void>>();
    yield* Effect.addFinalizer(() =>
      Effect.forEach(watchers.values(), Fiber.interrupt, { discard: true }),
    );
    yield* SubscriptionRef.changes(registry.entries).pipe(
      Stream.runForEach(
        Effect.fn("clientRuntime.connection.reconcileHubWatchers")(function* (entries) {
          const candidates = new Set(
            [...entries]
              .filter(([, entry]) => entry.enabled && entry.target._tag !== "HubConnectionTarget")
              .map(([environmentId]) => environmentId),
          );
          for (const [environmentId, fiber] of watchers) {
            if (candidates.has(environmentId)) continue;
            yield* Fiber.interrupt(fiber);
            watchers.delete(environmentId);
            if (!entries.has(environmentId)) {
              yield* reconcileHubInventory(environmentId, { environments: [] }).pipe(Effect.ignore);
            }
          }
          for (const environmentId of candidates) {
            if (watchers.has(environmentId)) continue;
            const watcher = registry
              .followStream(
                environmentId,
                Stream.unwrap(
                  Effect.map(EnvironmentSupervisor.EnvironmentSupervisor, (supervisor) =>
                    SubscriptionRef.changes(supervisor.prepared),
                  ),
                ),
              )
              .pipe(
                Stream.switchMap((prepared) => {
                  if (Option.isNone(prepared))
                    return Stream.fromEffect(markHubUnavailable(environmentId));
                  const parent = prepared.value;
                  return Stream.fromEffectSchedule(
                    SubscriptionRef.update(hubs, (current) =>
                      new Map(current).set(environmentId, parent),
                    ).pipe(
                      Effect.andThen(fetchHubInventory(parent)),
                      Effect.flatMap((inventory) =>
                        reconcileHubInventory(environmentId, inventory),
                      ),
                      Effect.as(true),
                      Effect.catch((error) => {
                        const unsupported =
                          error._tag === "RemoteEnvironmentAuthUndeclaredStatusError" &&
                          error.status === 404;
                        return unsupported || error._tag === "EnvironmentAuthInvalidError"
                          ? markHubUnavailable(environmentId).pipe(Effect.as(!unsupported))
                          : Effect.succeed(true);
                      }),
                    ),
                    Schedule.spaced("15 seconds"),
                  ).pipe(
                    Stream.takeUntil((supported) => !supported),
                    Stream.map(() => undefined),
                  );
                }),
                Stream.runDrain,
                Effect.ensuring(markHubUnavailable(environmentId)),
                Effect.forkScoped,
              );
            watchers.set(environmentId, yield* watcher);
          }
        }),
      ),
    );
  },
);
