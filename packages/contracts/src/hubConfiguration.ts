import * as Schema from "effect/Schema";
import { EnvironmentId } from "./baseSchemas.ts";
import { ProjectSnapshot, ProjectMutation } from "./project.ts";
import { ProviderInstanceMutation } from "./providerInstance.ts";
import {
  ServerSettings,
  ServerSettingsPatch,
  requiredScopesForProjectMutation,
  requiredScopesForServerSettingsPatch,
} from "./settings.ts";
import {
  AuthAccessWriteScope,
  AuthOrchestrationOperateScope,
  AuthProvidersManageScope,
  AuthSettingsWriteScope,
} from "./auth.ts";
import {
  ScheduledTaskListResult,
  ScheduledTaskDeleteInput,
  ScheduledTaskSetEnabledInput,
  ScheduledTaskUpsertInput,
} from "./scheduledTask.ts";

export const HubConfigurationIntent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("settings.patch"), patch: ServerSettingsPatch }),
  Schema.Struct({ type: Schema.Literal("provider.mutate"), mutation: ProviderInstanceMutation }),
  ProjectMutation,
  Schema.Struct({ type: Schema.Literal("automation.upsert"), input: ScheduledTaskUpsertInput }),
  Schema.Struct({ type: Schema.Literal("automation.delete"), input: ScheduledTaskDeleteInput }),
  Schema.Struct({
    type: Schema.Literal("automation.setEnabled"),
    input: ScheduledTaskSetEnabledInput,
  }),
]);
export type HubConfigurationIntent = typeof HubConfigurationIntent.Type;

export const HUB_CONFIGURATION_MANAGEMENT_SCOPES = [
  AuthAccessWriteScope,
  AuthSettingsWriteScope,
  AuthProvidersManageScope,
  AuthOrchestrationOperateScope,
] as const;

export function requiredScopesForHubConfigurationIntent(intent: HubConfigurationIntent) {
  const scopes =
    intent.type === "settings.patch"
      ? requiredScopesForServerSettingsPatch(intent.patch)
      : intent.type === "provider.mutate"
        ? ([AuthProvidersManageScope] as const)
        : intent.type === "project.create" ||
            intent.type === "project.update" ||
            intent.type === "project.delete"
          ? requiredScopesForProjectMutation(intent)
          : ([AuthOrchestrationOperateScope] as const);
  return [AuthAccessWriteScope, ...scopes];
}
export const HubConfigurationEnqueue = Schema.Struct({
  environmentId: EnvironmentId,
  expectedRevision: Schema.Number,
  requestId: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
  intent: HubConfigurationIntent,
  expectedHash: Schema.optional(Schema.String),
});
export type HubConfigurationEnqueue = typeof HubConfigurationEnqueue.Type;
export const HubConfigurationApply = Schema.Struct({
  requestId: Schema.String,
  intent: HubConfigurationIntent,
  expectedHash: Schema.optional(Schema.String),
});
export type HubConfigurationApply = typeof HubConfigurationApply.Type;
export const HubConfigurationEntry = Schema.Struct({
  requestId: Schema.String,
  revision: Schema.Number,
  type: Schema.String,
  status: Schema.Literals(["pending", "applied", "failed", "cancelled"]),
  error: Schema.NullOr(Schema.String),
});
export const HubConfigurationState = Schema.Struct({
  revision: Schema.Number,
  entries: Schema.Array(HubConfigurationEntry),
});
export type HubConfigurationState = typeof HubConfigurationState.Type;

export const HubObservedConfiguration = Schema.Struct({
  settings: ServerSettings,
  projects: ProjectSnapshot,
  automation: ScheduledTaskListResult,
  hashes: Schema.Record(Schema.String, Schema.String),
});
export type HubObservedConfiguration = typeof HubObservedConfiguration.Type;
