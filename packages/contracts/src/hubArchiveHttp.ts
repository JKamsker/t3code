import * as Schema from "effect/Schema";
import { EnvironmentId, ThreadId, TrimmedNonEmptyString, TurnItemId } from "./baseSchemas.ts";

export const HubArchiveEnvironmentParams = Schema.Struct({ environmentId: EnvironmentId });
export const HubArchiveThreadParams = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
});
export const HubArchiveItemParams = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
  itemId: TurnItemId,
});
export const HubArchiveAttachmentParams = Schema.Struct({
  environmentId: EnvironmentId,
  attachmentId: TrimmedNonEmptyString,
});
export const HubArchiveShellQuery = Schema.Struct({
  location: Schema.optionalKey(Schema.Literals(["active", "archive"])),
});
export const HubArchiveHistoryQuery = Schema.Struct({ cursor: TrimmedNonEmptyString });
export const HubArchiveStatus = Schema.Struct({
  storeId: Schema.NullOr(Schema.String),
  ackSequence: Schema.Number,
  anchorEventId: Schema.NullOr(Schema.String),
  complete: Schema.Boolean,
  lastSyncedAt: Schema.NullOr(Schema.String),
  missingAssets: Schema.Number,
  syncError: Schema.NullOr(Schema.String),
});
