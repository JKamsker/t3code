import * as Schema from "effect/Schema";

import { EnvironmentId, NonNegativeInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ExecutionEnvironmentDescriptor } from "./environment.ts";
import { ApplicationProjectEvent } from "./applicationEvent.ts";
import { OrchestrationV2StoredEvent } from "./orchestrationV2.ts";
import { AuthEnvironmentScope } from "./auth.ts";
import { ChatAttachment } from "./chatAttachment.ts";

export const HubEnvironmentSummary = Schema.Struct({
  environmentId: EnvironmentId,
  label: TrimmedNonEmptyString,
  environment: ExecutionEnvironmentDescriptor,
  online: Schema.Boolean,
  lastSeenAt: Schema.NullOr(Schema.String),
});
export type HubEnvironmentSummary = typeof HubEnvironmentSummary.Type;

export const HubInventory = Schema.Struct({ environments: Schema.Array(HubEnvironmentSummary) });
export type HubInventory = typeof HubInventory.Type;

export const HubCreateEnrollment = Schema.Struct({
  label: Schema.optionalKey(TrimmedNonEmptyString),
});
export const HubEnrollmentResult = Schema.Struct({
  enrollmentId: TrimmedNonEmptyString,
  code: TrimmedNonEmptyString,
  expiresAt: Schema.String,
});
export type HubEnrollmentResult = typeof HubEnrollmentResult.Type;
export const HubEnrollRequest = Schema.Struct({
  code: TrimmedNonEmptyString,
  environment: ExecutionEnvironmentDescriptor,
});
export type HubEnrollRequest = typeof HubEnrollRequest.Type;
export const HubEnrollResult = Schema.Struct({
  hubId: EnvironmentId,
  workerId: TrimmedNonEmptyString,
  workerToken: TrimmedNonEmptyString,
});
export type HubEnrollResult = typeof HubEnrollResult.Type;

export const HubArchiveEvent = Schema.Union([ApplicationProjectEvent, OrchestrationV2StoredEvent]);
export const HubArchiveBatch = Schema.Struct({
  storeId: TrimmedNonEmptyString,
  afterSequence: NonNegativeInt,
  throughSequence: NonNegativeInt,
  anchorEventId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  events: Schema.Array(HubArchiveEvent),
  bootstrap: Schema.optionalKey(Schema.Boolean),
  complete: Schema.optionalKey(Schema.Boolean),
  assets: Schema.optionalKey(
    Schema.Array(Schema.Struct({ attachment: ChatAttachment, data: Schema.NullOr(Schema.String) })),
  ),
});
export type HubArchiveBatch = typeof HubArchiveBatch.Type;
export const HubArchiveAck = Schema.Struct({
  storeId: TrimmedNonEmptyString,
  ackSequence: NonNegativeInt,
  anchorEventId: Schema.optionalKey(Schema.NullOr(Schema.String)),
});
export type HubArchiveAck = typeof HubArchiveAck.Type;

const HeaderMap = Schema.Record(Schema.String, Schema.String);
const Channel = { channelId: TrimmedNonEmptyString };
export const HubWorkerFrame = Schema.Union([
  Schema.Struct({ type: Schema.Literal("hello"), environment: ExecutionEnvironmentDescriptor }),
  Schema.Struct({ type: Schema.Literal("ping") }),
  Schema.Struct({ type: Schema.Literal("pong") }),
  Schema.Struct({
    type: Schema.Literal("http.open"),
    ...Channel,
    method: Schema.String,
    path: Schema.String,
    headers: HeaderMap,
    scopes: Schema.Array(AuthEnvironmentScope),
  }),
  Schema.Struct({
    type: Schema.Literal("http.response"),
    ...Channel,
    status: Schema.Number,
    headers: HeaderMap,
  }),
  Schema.Struct({ type: Schema.Literal("http.data"), ...Channel, data: Schema.String }),
  Schema.Struct({ type: Schema.Literal("http.end"), ...Channel }),
  Schema.Struct({
    type: Schema.Literal("ws.open"),
    ...Channel,
    path: Schema.String,
    scopes: Schema.Array(AuthEnvironmentScope),
  }),
  Schema.Struct({
    type: Schema.Literal("ws.data"),
    ...Channel,
    data: Schema.String,
    binary: Schema.Boolean,
  }),
  Schema.Struct({ type: Schema.Literal("close"), ...Channel }),
  Schema.Struct({
    type: Schema.Literal("request"),
    requestId: TrimmedNonEmptyString,
    method: Schema.String,
    payload: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.Literal("response"),
    requestId: TrimmedNonEmptyString,
    success: Schema.Boolean,
    payload: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.Literal("archive.batch"),
    requestId: TrimmedNonEmptyString,
    batch: HubArchiveBatch,
  }),
  Schema.Struct({
    type: Schema.Literal("archive.ack"),
    requestId: TrimmedNonEmptyString,
    ack: HubArchiveAck,
  }),
  Schema.Struct({
    type: Schema.Literal("archive.chunk"),
    requestId: TrimmedNonEmptyString,
    index: NonNegativeInt,
    total: NonNegativeInt,
    data: Schema.String,
  }),
]);
export type HubWorkerFrame = typeof HubWorkerFrame.Type;
