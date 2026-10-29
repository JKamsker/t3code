import {
  EnvironmentId,
  HubArchiveEvent,
  type HubArchiveAck,
  type HubArchiveBatch,
  type OrchestrationV2GetTurnItemResult,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadHistoryPage,
  type OrchestrationV2ThreadStreamItem,
  type ThreadId,
  type TurnItemId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as LayerMap from "effect/LayerMap";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { buildBoundedThreadStreamSnapshot } from "../orchestration-v2/ThreadStream.ts";
import { THREAD_HISTORY_SNAPSHOT_ROW_LIMIT } from "../orchestration-v2/threadHistoryPaging.ts";

export class HubArchiveError extends Schema.TaggedError<HubArchiveError>()("HubArchiveError", {
  environmentId: EnvironmentId,
  reason: Schema.Literals(["storage", "cursor-mismatch", "source-changed", "invalid-batch"]),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    switch (this.reason) {
      case "cursor-mismatch":
        return "The archive cursor does not match. Reconnect this machine to resume syncing.";
      case "source-changed":
        return "This machine's session history changed. Its existing archive is preserved and syncing has stopped.";
      case "invalid-batch":
        return "The machine sent an invalid archive batch.";
      case "storage":
        return "The hub could not read or save this machine's archive.";
    }
  }
}

type ArchiveStatus = {
  readonly storeId: string | null;
  readonly ackSequence: number;
  readonly anchorEventId: string | null;
  readonly complete: boolean;
  readonly lastSyncedAt: string | null;
  readonly missingAssets: number;
  readonly syncError: string | null;
};

export class HubArchive extends Context.Service<
  HubArchive,
  {
    readonly receiveBatch: (
      environmentId: EnvironmentId,
      batch: HubArchiveBatch,
    ) => Effect.Effect<HubArchiveAck, HubArchiveError>;
    readonly status: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<ArchiveStatus, HubArchiveError>;
    readonly reportError: (
      environmentId: EnvironmentId,
      reason: "source-changed" | "cursor-mismatch" | "storage",
    ) => Effect.Effect<void, HubArchiveError>;
    readonly shell: (
      environmentId: EnvironmentId,
      location?: "active" | "archive",
    ) => Effect.Effect<OrchestrationV2ShellSnapshot, HubArchiveError>;
    readonly thread: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
    ) => Effect.Effect<
      Extract<OrchestrationV2ThreadStreamItem, { kind: "snapshot" }>,
      HubArchiveError
    >;
    readonly history: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      cursor: string,
    ) => Effect.Effect<OrchestrationV2ThreadHistoryPage, HubArchiveError>;
    readonly item: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      itemId: TurnItemId,
    ) => Effect.Effect<OrchestrationV2GetTurnItemResult, HubArchiveError>;
    readonly asset: (
      environmentId: EnvironmentId,
      attachmentId: string,
    ) => Effect.Effect<
      { readonly bytes: Uint8Array; readonly mimeType: string } | null,
      HubArchiveError
    >;
  }
>()("t3/hub/HubArchive") {}

const encodeEvent = Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(HubArchiveEvent)));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const isHubArchiveError = Schema.is(HubArchiveError);

const layerMirrorSetup = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // A replication acknowledgement must survive a power failure before its source discards data.
    yield* sql`PRAGMA synchronous = FULL`;
    yield* sql`CREATE TABLE IF NOT EXISTS hub_archive_metadata (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      store_id TEXT,
      ack_sequence INTEGER NOT NULL DEFAULT 0,
      anchor_event_id TEXT,
      complete INTEGER NOT NULL DEFAULT 0,
      last_synced_at TEXT,
      sync_error TEXT
    )`;
    const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(hub_archive_metadata)`;
    if (!columns.some((column) => column.name === "anchor_event_id")) {
      yield* sql`ALTER TABLE hub_archive_metadata ADD COLUMN anchor_event_id TEXT`;
    }
    if (!columns.some((column) => column.name === "sync_error")) {
      yield* sql`ALTER TABLE hub_archive_metadata ADD COLUMN sync_error TEXT`;
    }
    yield* sql`INSERT OR IGNORE INTO hub_archive_metadata(singleton) VALUES (1)`;
    yield* sql`UPDATE hub_archive_metadata SET anchor_event_id = (
      SELECT event_id FROM orchestration_events WHERE sequence = ack_sequence
    ) WHERE ack_sequence > 0 AND anchor_event_id IS NULL`;
    yield* sql`CREATE TABLE IF NOT EXISTS hub_archive_assets (
      attachment_id TEXT PRIMARY KEY,
      data BLOB,
      mime_type TEXT NOT NULL,
      missing INTEGER NOT NULL
    )`;
  }),
);

const readStatus = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly storeId: string | null;
    readonly ackSequence: number;
    readonly anchorEventId: string | null;
    readonly complete: number;
    readonly lastSyncedAt: string | null;
    readonly missingAssets: number;
    readonly syncError: string | null;
  }>`SELECT store_id AS "storeId", ack_sequence AS "ackSequence", complete,
    anchor_event_id AS "anchorEventId", sync_error AS "syncError",
    last_synced_at AS "lastSyncedAt",
    (SELECT COUNT(*) FROM hub_archive_assets WHERE missing = 1) AS "missingAssets"
    FROM hub_archive_metadata WHERE singleton = 1`;
  const row = rows[0]!;
  return { ...row, complete: row.complete === 1 } satisfies ArchiveStatus;
});

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const archiveRoot = path.join(config.stateDir, "hub", "archives");
  const directory = Effect.fn("HubArchive.directory")(function* (environmentId: EnvironmentId) {
    const hash = yield* crypto.digest("SHA-256", new TextEncoder().encode(environmentId));
    return path.join(archiveRoot, Hex.encode(hash));
  });
  const mirrors = yield* LayerMap.make(
    (environmentId: EnvironmentId) =>
      Layer.unwrap(
        directory(environmentId).pipe(
          Effect.map((root) => {
            const database = Sqlite.layerFromPath(path.join(root, "archive.sqlite"));
            return Layer.fresh(
              Layer.mergeAll(ProjectStore.layer, ProjectionStore.layer, layerMirrorSetup).pipe(
                Layer.provideMerge(database),
              ),
            );
          }),
        ),
      ),
    { idleTimeToLive: "1 minute" },
  );
  const inMirror = <A, E>(
    environmentId: EnvironmentId,
    operation: Effect.Effect<
      A,
      E,
      SqlClient.SqlClient | ProjectStore.ProjectStoreV2 | ProjectionStore.ProjectionStoreV2
    >,
  ) =>
    operation.pipe(
      Effect.provide(mirrors.get(environmentId)),
      Effect.mapError((cause) =>
        isHubArchiveError(cause)
          ? cause
          : new HubArchiveError({ environmentId, reason: "storage", cause }),
      ),
    );

  const receiveBatch = Effect.fn("HubArchive.receiveBatch")(function* (
    environmentId: EnvironmentId,
    batch: HubArchiveBatch,
  ) {
    return yield* inMirror(
      environmentId,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const projects = yield* ProjectStore.ProjectStoreV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const status = yield* readStatus;
        if (status.storeId !== null && status.storeId !== batch.storeId) {
          return yield* new HubArchiveError({ environmentId, reason: "source-changed" });
        }
        if (batch.afterSequence > status.ackSequence) {
          return yield* new HubArchiveError({ environmentId, reason: "cursor-mismatch" });
        }
        let previous = batch.afterSequence;
        for (const stored of batch.events) {
          if (stored.sequence <= previous || stored.sequence > batch.throughSequence) {
            return yield* new HubArchiveError({ environmentId, reason: "invalid-batch" });
          }
          previous = stored.sequence;
        }
        if (batch.throughSequence < batch.afterSequence) {
          return yield* new HubArchiveError({ environmentId, reason: "invalid-batch" });
        }
        return yield* sql.withTransaction(
          Effect.gen(function* () {
            const current = yield* readStatus;
            if (current.storeId !== null && current.storeId !== batch.storeId) {
              return yield* new HubArchiveError({ environmentId, reason: "source-changed" });
            }
            if (batch.afterSequence > current.ackSequence) {
              return yield* new HubArchiveError({ environmentId, reason: "cursor-mismatch" });
            }
            if (
              batch.bootstrap === true &&
              batch.complete === true &&
              batch.throughSequence < current.ackSequence
            ) {
              return yield* new HubArchiveError({ environmentId, reason: "source-changed" });
            }
            let anchorEventId = current.anchorEventId;
            if (batch.throughSequence > current.ackSequence) {
              const last = batch.events.at(-1);
              if (last === undefined || last.sequence !== batch.throughSequence) {
                return yield* new HubArchiveError({ environmentId, reason: "invalid-batch" });
              }
              anchorEventId = "event" in last ? last.event.id : last.eventId;
            }
            if (
              batch.throughSequence >= current.ackSequence &&
              batch.anchorEventId !== undefined &&
              batch.anchorEventId !== anchorEventId
            ) {
              return yield* new HubArchiveError({ environmentId, reason: "source-changed" });
            }
            for (const asset of batch.assets ?? []) {
              const bytes = asset.data === null ? null : Buffer.from(asset.data, "base64");
              if (bytes !== null && bytes.length !== asset.attachment.sizeBytes) {
                return yield* new HubArchiveError({ environmentId, reason: "invalid-batch" });
              }
              yield* sql`INSERT INTO hub_archive_assets(attachment_id, data, mime_type, missing)
                VALUES (${asset.attachment.id}, ${bytes}, ${asset.attachment.mimeType}, ${bytes === null ? 1 : 0})
                ON CONFLICT(attachment_id) DO UPDATE SET
                  data = COALESCE(excluded.data, hub_archive_assets.data),
                  mime_type = CASE WHEN excluded.data IS NULL AND hub_archive_assets.data IS NOT NULL
                    THEN hub_archive_assets.mime_type ELSE excluded.mime_type END,
                  missing = CASE WHEN COALESCE(excluded.data, hub_archive_assets.data) IS NULL
                    THEN 1 ELSE 0 END`;
            }
            for (const stored of batch.events) {
              if (stored.sequence <= current.ackSequence) continue;
              const encoded = yield* encodeEvent(stored);
              const agent = "event" in stored;
              const event = agent ? stored.event : stored;
              const eventId = agent ? stored.event.id : stored.eventId;
              const streamId = agent ? stored.event.threadId : stored.aggregateId;
              const metadata = yield* encodeJson(
                "event" in stored
                  ? {
                      runId: stored.event.runId,
                      nodeId: stored.event.nodeId,
                      driver: stored.event.driver,
                      providerInstanceId: stored.event.providerInstanceId,
                      rawEventId: stored.event.rawEventId,
                    }
                  : stored.metadata,
              );
              const occurredAt =
                "event" in stored ? DateTime.formatIso(stored.event.occurredAt) : stored.occurredAt;
              yield* sql`INSERT INTO orchestration_events (
                sequence, event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
                payload_json, metadata_json, application_event_version
              ) VALUES (${stored.sequence}, ${eventId}, ${agent ? "thread" : "project"}, ${streamId},
                ${stored.sequence}, ${event.type}, ${occurredAt}, ${stored.commandId},
                NULL, ${stored.commandId}, 'server',
                json_extract(${encoded}, ${agent ? "$.event.payload" : "$.payload"}), ${metadata}, 2)`;
              if ("event" in stored) yield* projections.apply(stored.event);
              else yield* projects.apply(stored);
            }
            const now = DateTime.formatIso(yield* DateTime.now);
            const ackSequence = Math.max(current.ackSequence, batch.throughSequence);
            yield* sql`UPDATE hub_archive_metadata SET store_id = ${batch.storeId},
              ack_sequence = ${ackSequence}, anchor_event_id = ${anchorEventId},
              complete = ${batch.complete === true || current.complete ? 1 : 0},
              last_synced_at = ${now}, sync_error = NULL WHERE singleton = 1`;
            return { storeId: batch.storeId, ackSequence, anchorEventId } satisfies HubArchiveAck;
          }),
        );
      }),
    );
  });

  return HubArchive.of({
    receiveBatch,
    status: Effect.fn("HubArchive.status")((environmentId) => inMirror(environmentId, readStatus)),
    reportError: Effect.fn("HubArchive.reportError")((environmentId, reason) =>
      inMirror(
        environmentId,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const error = new HubArchiveError({ environmentId, reason });
          yield* sql`UPDATE hub_archive_metadata SET sync_error = ${error.message} WHERE singleton = 1`;
        }),
      ),
    ),
    shell: Effect.fn("HubArchive.shell")((environmentId, location) =>
      inMirror(
        environmentId,
        Effect.gen(function* () {
          const projects = yield* ProjectStore.ProjectStoreV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const snapshot = yield* projections.getShellSnapshot(
            location === undefined ? undefined : { location },
          );
          return {
            ...snapshot,
            projects: yield* projects.listShells(),
            snapshotSequence: (yield* readStatus).ackSequence,
          };
        }),
      ),
    ),
    thread: Effect.fn("HubArchive.thread")((environmentId, threadId) =>
      inMirror(
        environmentId,
        Effect.gen(function* () {
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const snapshot = yield* projections.getThreadSnapshotWindow(threadId, {
            rowLimit: THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
          });
          return buildBoundedThreadStreamSnapshot(snapshot);
        }),
      ),
    ),
    history: Effect.fn("HubArchive.history")((environmentId, threadId, cursor) =>
      inMirror(
        environmentId,
        ProjectionStore.ProjectionStoreV2.pipe(
          Effect.flatMap((projections) => projections.getThreadHistoryPage(threadId, cursor)),
        ),
      ),
    ),
    item: Effect.fn("HubArchive.item")((environmentId, threadId, itemId) =>
      inMirror(
        environmentId,
        ProjectionStore.ProjectionStoreV2.pipe(
          Effect.flatMap((projections) => projections.getTurnItem({ threadId, itemId })),
          Effect.map((item) => ({ item })),
        ),
      ),
    ),
    asset: Effect.fn("HubArchive.asset")((environmentId, attachmentId) =>
      inMirror(
        environmentId,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const rows = yield* sql<{ readonly bytes: Uint8Array; readonly mimeType: string }>`
            SELECT data AS bytes, mime_type AS "mimeType"
            FROM hub_archive_assets WHERE attachment_id = ${attachmentId} AND missing = 0`;
          const row = rows[0];
          if (row === undefined) return null;
          return row;
        }),
      ),
    ),
  });
});

export const layer = Layer.effect(HubArchive, make);
