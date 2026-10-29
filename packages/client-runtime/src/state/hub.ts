import {
  AuthAccessWriteScope,
  type EnvironmentId,
  type HubConfigurationEnqueue,
  type ThreadId,
  type TurnItemId,
  HUB_CONFIGURATION_MANAGEMENT_SCOPES,
  HubConfigurationEnqueue as HubConfigurationEnqueueSchema,
  requiredScopesForHubConfigurationIntent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Schema from "effect/Schema";
import type { HttpClient } from "effect/http";
import { Atom } from "effect/reactivity";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import { fetchHubInventory } from "../connection/hubDiscovery.ts";
import { createScopePermissions } from "./commandPermissions.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "./environmentHttpAuth.ts";
import { createEnvironmentCommand, createEnvironmentQueryAtomFamily } from "./runtime.ts";

const isHubConfigurationEnqueue = Schema.is(HubConfigurationEnqueueSchema);

const currentHubConnection = Effect.fn("clientRuntime.state.currentHubConnection")(function* () {
  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
  const prepared = yield* SubscriptionRef.get(supervisor.prepared);
  if (Option.isNone(prepared))
    return yield* new EnvironmentRpcUnavailableError({
      environmentId: supervisor.target.environmentId,
      message: "The home server is not connected.",
    });
  const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(
    RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization,
  );
  return { prepared: prepared.value, signer, remoteAuthorization };
});

export function createHubEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | HttpClient.HttpClient | R, E>,
) {
  const permissions = createScopePermissions(runtime, [AuthAccessWriteScope]);
  const managementPermissions = createScopePermissions(
    runtime,
    HUB_CONFIGURATION_MANAGEMENT_SCOPES,
  );
  const enqueuePermissions = createScopePermissions(runtime, (input) =>
    isHubConfigurationEnqueue(input)
      ? requiredScopesForHubConfigurationIntent(input.intent)
      : HUB_CONFIGURATION_MANAGEMENT_SCOPES,
  );
  const inventory = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:inventory",
    staleTimeMs: 5_000,
    refreshIntervalMs: 15_000,
    execute: Effect.fn("clientRuntime.state.hubInventory")(function* (_input: {}) {
      const { prepared } = yield* currentHubConnection();
      return yield* fetchHubInventory(prepared);
    }),
  });
  const configuration = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:configuration",
    staleTimeMs: 2_000,
    refreshIntervalMs: 5_000,
    execute: Effect.fn("clientRuntime.state.hubConfiguration")(function* (input: {
      readonly environmentId: EnvironmentId;
    }) {
      const connection = yield* currentHubConnection();
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubConfiguration",
        method: "GET",
        timeoutMs: 10_000,
        url: (urls) => urls.configuration({ params: input }),
        request: ({ client, headers }) => client.configuration({ params: input, headers }),
      });
    }),
  });
  const observedConfiguration = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:observed-configuration",
    staleTimeMs: 5_000,
    refreshIntervalMs: 15_000,
    execute: Effect.fn("clientRuntime.state.hubObservedConfiguration")(function* (input: {
      readonly environmentId: EnvironmentId;
    }) {
      const connection = yield* currentHubConnection();
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubConfiguration",
        method: "GET",
        timeoutMs: 10_000,
        url: (urls) => urls.observedConfiguration({ params: input }),
        request: ({ client, headers }) => client.observedConfiguration({ params: input, headers }),
      });
    }),
  });
  const archiveStatus = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:archive-status",
    staleTimeMs: 5_000,
    refreshIntervalMs: 15_000,
    execute: Effect.fn("clientRuntime.state.hubArchiveStatus")(function* (input: {
      readonly environmentId: EnvironmentId;
    }) {
      const connection = yield* currentHubConnection();
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubArchive",
        method: "GET",
        timeoutMs: 10_000,
        url: (urls) => urls.archiveStatus({ params: input }),
        request: ({ client, headers }) => client.archiveStatus({ params: input, headers }),
      });
    }),
  });
  const archiveShell = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:archive-shell",
    staleTimeMs: 5_000,
    execute: Effect.fn("clientRuntime.state.hubArchiveShell")(function* (input: {
      readonly environmentId: EnvironmentId;
      readonly location?: "active" | "archive";
    }) {
      const connection = yield* currentHubConnection();
      const endpoint = {
        params: { environmentId: input.environmentId },
        query: input.location === undefined ? {} : { location: input.location },
      };
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubArchive",
        method: "GET",
        timeoutMs: 30_000,
        url: (urls) => urls.archiveShell(endpoint),
        request: ({ client, headers }) => client.archiveShell({ ...endpoint, headers }),
      });
    }),
  });
  const archiveThread = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:archive-thread",
    staleTimeMs: 5_000,
    execute: Effect.fn("clientRuntime.state.hubArchiveThread")(function* (input: {
      readonly environmentId: EnvironmentId;
      readonly threadId: ThreadId;
    }) {
      const connection = yield* currentHubConnection();
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubArchive",
        method: "GET",
        timeoutMs: 30_000,
        url: (urls) => urls.archiveThread({ params: input }),
        request: ({ client, headers }) => client.archiveThread({ params: input, headers }),
      });
    }),
  });
  const archiveHistory = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:archive-history",
    staleTimeMs: 5_000,
    execute: Effect.fn("clientRuntime.state.hubArchiveHistory")(function* (input: {
      readonly environmentId: EnvironmentId;
      readonly threadId: ThreadId;
      readonly cursor: string;
    }) {
      const connection = yield* currentHubConnection();
      const endpoint = {
        params: { environmentId: input.environmentId, threadId: input.threadId },
        query: { cursor: input.cursor },
      };
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubArchive",
        method: "GET",
        timeoutMs: 30_000,
        url: (urls) => urls.archiveHistory(endpoint),
        request: ({ client, headers }) => client.archiveHistory({ ...endpoint, headers }),
      });
    }),
  });
  const archiveTurnItem = createEnvironmentQueryAtomFamily(runtime, {
    label: "environment-data:hub:archive-turn-item",
    staleTimeMs: 5_000,
    execute: Effect.fn("clientRuntime.state.hubArchiveTurnItem")(function* (input: {
      readonly environmentId: EnvironmentId;
      readonly threadId: ThreadId;
      readonly turnItemId: TurnItemId;
    }) {
      const connection = yield* currentHubConnection();
      const params = {
        environmentId: input.environmentId,
        threadId: input.threadId,
        itemId: input.turnItemId,
      };
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubArchive",
        method: "GET",
        timeoutMs: 30_000,
        url: (urls) => urls.archiveTurnItem({ params }),
        request: ({ client, headers }) => client.archiveTurnItem({ params, headers }),
      });
    }),
  });
  const archiveAttachment = createEnvironmentCommand(runtime, {
    label: "environment-data:hub:archive-attachment",
    execute: Effect.fn("clientRuntime.state.hubArchiveAttachment")(function* (input: {
      readonly environmentId: EnvironmentId;
      readonly attachmentId: string;
    }) {
      const connection = yield* currentHubConnection();
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubArchive",
        method: "GET",
        timeoutMs: 30_000,
        url: (urls) => urls.archiveAttachment({ params: input }),
        request: ({ client, headers }) => client.archiveAttachment({ params: input, headers }),
      });
    }),
  });
  const createEnrollment = createEnvironmentCommand(runtime, {
    label: "environment-data:hub:create-enrollment",
    execute: Effect.fn("clientRuntime.state.createHubEnrollment")(function* (
      input: { readonly label?: string },
      registry,
      environmentId,
    ) {
      yield* permissions.authorize(registry, environmentId);
      const connection = yield* currentHubConnection();
      return yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hub",
        method: "POST",
        timeoutMs: 10_000,
        url: (urls) => urls.createEnrollment(),
        request: ({ client, headers }) => client.createEnrollment({ payload: input, headers }),
      });
    }),
  });
  const removeEnvironment = createEnvironmentCommand(runtime, {
    label: "environment-data:hub:remove-environment",
    execute: Effect.fn("clientRuntime.state.removeHubEnvironment")(function* (
      input: { readonly environmentId: EnvironmentId },
      registry,
      environmentId,
    ) {
      yield* permissions.authorize(registry, environmentId);
      const connection = yield* currentHubConnection();
      const result = yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hub",
        method: "DELETE",
        timeoutMs: 10_000,
        url: (urls) => urls.removeEnvironment({ params: input }),
        request: ({ client, headers }) => client.removeEnvironment({ params: input, headers }),
      });
      registry.refresh(inventory({ environmentId, input: {} }));
      return result;
    }),
  });
  const enqueueConfiguration = createEnvironmentCommand(runtime, {
    label: "environment-data:hub:enqueue-configuration",
    execute: Effect.fn("clientRuntime.state.enqueueHubConfiguration")(function* (
      input: HubConfigurationEnqueue,
      registry,
      environmentId,
    ) {
      yield* enqueuePermissions.authorize(registry, environmentId, input);
      const connection = yield* currentHubConnection();
      const params = { environmentId: input.environmentId };
      const result = yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubConfiguration",
        method: "POST",
        timeoutMs: 10_000,
        url: (urls) => urls.enqueueConfiguration({ params }),
        request: ({ client, headers }) =>
          client.enqueueConfiguration({ params, payload: input, headers }),
      });
      registry.refresh(configuration({ environmentId, input: params }));
      return result;
    }),
  });
  const cancelConfiguration = createEnvironmentCommand(runtime, {
    label: "environment-data:hub:cancel-configuration",
    execute: Effect.fn("clientRuntime.state.cancelHubConfiguration")(function* (
      input: { readonly environmentId: EnvironmentId; readonly requestId: string },
      registry,
      environmentId,
    ) {
      yield* managementPermissions.authorize(registry, environmentId);
      const connection = yield* currentHubConnection();
      const params = { environmentId: input.environmentId };
      const result = yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubConfiguration",
        method: "POST",
        timeoutMs: 10_000,
        url: (urls) => urls.cancelConfiguration({ params }),
        request: ({ client, headers }) =>
          client.cancelConfiguration({ params, payload: { requestId: input.requestId }, headers }),
      });
      registry.refresh(configuration({ environmentId, input: params }));
      return result;
    }),
  });
  const retryConfiguration = createEnvironmentCommand(runtime, {
    label: "environment-data:hub:retry-configuration",
    execute: Effect.fn("clientRuntime.state.retryHubConfiguration")(function* (
      input: { readonly environmentId: EnvironmentId },
      registry,
      environmentId,
    ) {
      yield* managementPermissions.authorize(registry, environmentId);
      const connection = yield* currentHubConnection();
      const result = yield* executeAuthenticatedEnvironmentHttpRequest({
        ...connection,
        group: "hubConfiguration",
        method: "POST",
        timeoutMs: 10_000,
        url: (urls) => urls.retryConfiguration({ params: input }),
        request: ({ client, headers }) => client.retryConfiguration({ params: input, headers }),
      });
      registry.refresh(configuration({ environmentId, input }));
      return result;
    }),
  });
  return {
    inventory,
    configuration,
    observedConfiguration,
    archiveStatus,
    archiveShell,
    archiveThread,
    archiveHistory,
    archiveTurnItem,
    archiveAttachment,
    createEnrollment: { ...createEnrollment, permissionAtom: permissions.permissionAtom },
    removeEnvironment: { ...removeEnvironment, permissionAtom: permissions.permissionAtom },
    enqueueConfiguration: {
      ...enqueueConfiguration,
      permissionAtom: enqueuePermissions.permissionAtom,
    },
    cancelConfiguration: {
      ...cancelConfiguration,
      permissionAtom: managementPermissions.permissionAtom,
    },
    retryConfiguration: {
      ...retryConfiguration,
      permissionAtom: managementPermissions.permissionAtom,
    },
  };
}
