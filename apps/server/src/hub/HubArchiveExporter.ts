import {
  ChatAttachment,
  HubArchiveEvent,
  type HubArchiveBatch,
  type ApplicationStoredEvent,
} from "@t3tools/contracts";
import { htmlRenderFromToolItem, mcpAppFromToolItem } from "@t3tools/shared/toolOutput";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";

export class HubArchiveExportError extends Schema.TaggedError<HubArchiveExportError>()(
  "HubArchiveExportError",
  {
    cause: Schema.Defect(),
    reason: Schema.optional(Schema.Literals(["source-changed", "storage"])),
  },
) {
  override get message(): string {
    return "This machine could not export its session archive.";
  }
}

export class HubArchiveExporter extends Context.Service<
  HubArchiveExporter,
  {
    readonly storeId: string;
    readonly stream: (input: {
      readonly afterSequence?: number;
      readonly afterEventId?: string | null;
    }) => Stream.Stream<HubArchiveBatch, HubArchiveExportError>;
  }
>()("t3/hub/HubArchiveExporter") {}

const MAX_BATCH_BYTES = 256 * 1024;
const encodeEvent = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(HubArchiveEvent)));
const isHubArchiveExportError = Schema.is(HubArchiveExportError);
const isAttachment = Schema.is(ChatAttachment);

function referencedAttachments(events: ReadonlyArray<ApplicationStoredEvent>) {
  const found = new Map<string, { attachment: ChatAttachment; inferSize: boolean }>();
  const pending: unknown[] = [...events];
  while (pending.length > 0) {
    const value = pending.pop();
    if (typeof value !== "object" || value === null) continue;
    if (isAttachment(value)) {
      found.set(value.id, { attachment: value, inferSize: false });
      continue;
    }
    if ("toolName" in value && typeof value.toolName === "string") {
      const item = {
        toolName: value.toolName,
        output: "output" in value ? value.output : undefined,
      };
      const reference = htmlRenderFromToolItem(item) ?? mcpAppFromToolItem(item);
      if (reference !== undefined) {
        const attachment = {
          id: reference.attachmentId,
          type: "file" as const,
          name: `${reference.attachmentId}.html`,
          mimeType: "text/html",
          sizeBytes: 1,
        };
        if (isAttachment(attachment)) {
          found.set(attachment.id, { attachment, inferSize: true });
        }
      }
    }
    for (const nested of Object.values(value)) pending.push(nested);
  }
  return [...found.values()];
}

function splitBatches(events: ReadonlyArray<ApplicationStoredEvent>) {
  const groups: ApplicationStoredEvent[][] = [];
  let group: ApplicationStoredEvent[] = [];
  let size = 0;
  for (const event of events) {
    const bytes = Buffer.byteLength(encodeEvent(event));
    if (size + bytes > MAX_BATCH_BYTES && group.length > 0) {
      groups.push(group);
      group = [];
      size = 0;
    }
    group.push(event);
    size += bytes;
  }
  if (group.length > 0) groups.push(group);
  return groups;
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  yield* sql`CREATE TABLE IF NOT EXISTS hub_replication_source (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1), store_id TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS hub_replication_anchors (
    sequence INTEGER PRIMARY KEY, event_id TEXT NOT NULL
  )`;
  const candidate = yield* crypto.randomUUIDv4();
  yield* sql`INSERT OR IGNORE INTO hub_replication_source(singleton, store_id) VALUES (1, ${candidate})`;
  const source = yield* sql<{ readonly storeId: string }>`
    SELECT store_id AS "storeId" FROM hub_replication_source WHERE singleton = 1`;
  const storeId = source[0]!.storeId;
  const eventId = (stored: ApplicationStoredEvent) =>
    "event" in stored ? stored.event.id : stored.eventId;
  const rememberAnchor = Effect.fn("HubArchiveExporter.rememberAnchor")(function* (
    sequence: number,
    anchorEventId: string,
  ) {
    yield* sql`INSERT OR IGNORE INTO hub_replication_anchors(sequence, event_id)
      VALUES (${sequence}, ${anchorEventId})`;
    const rows = yield* sql<{ readonly eventId: string }>`SELECT event_id AS "eventId"
      FROM hub_replication_anchors WHERE sequence = ${sequence}`;
    if (rows[0]!.eventId !== anchorEventId) {
      return yield* new HubArchiveExportError({
        cause: new Error("Source history diverged"),
        reason: "source-changed",
      });
    }
  });
  const readAnchor = Effect.fn("HubArchiveExporter.readAnchor")(function* (sequence: number) {
    const rows = yield* sql<{ readonly eventId: string }>`SELECT event_id AS "eventId"
      FROM hub_replication_anchors WHERE sequence = ${sequence}`;
    const retained = yield* events
      .readApplicationEvents({ afterSequence: sequence - 1, throughSequence: sequence })
      .pipe(Stream.runHead);
    if (Option.isSome(retained)) {
      const id = eventId(retained.value);
      yield* rememberAnchor(sequence, id);
      return id;
    }
    return rows[0]?.eventId ?? null;
  });

  return HubArchiveExporter.of({
    storeId,
    stream: (input) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const highWater = yield* events.latestApplicationSequence;
          const afterSequence = input.afterSequence ?? 0;
          if (afterSequence > highWater) {
            return yield* new HubArchiveExportError({
              cause: new Error("Source cursor regressed"),
              reason: "source-changed",
            });
          }
          let anchorEventId = afterSequence === 0 ? null : yield* readAnchor(afterSequence);
          if (
            afterSequence > 0 &&
            (anchorEventId === null || input.afterEventId !== anchorEventId)
          ) {
            return yield* new HubArchiveExportError({
              cause: new Error("Source history no longer matches the hub archive"),
              reason: "source-changed",
            });
          }
          const sentAttachments = new Set<string>();
          let cursor = afterSequence;
          const batches = (
            source: Stream.Stream<ApplicationStoredEvent, unknown>,
            bootstrap: boolean,
          ) =>
            source.pipe(
              Stream.groupedWithin(50, "100 millis"),
              Stream.flatMap((group) => Stream.fromIterable(splitBatches(group))),
              Stream.flatMap((group) => {
                const assets = Stream.fromIterable(referencedAttachments(group)).pipe(
                  Stream.filter(({ attachment }) => !sentAttachments.has(attachment.id)),
                  Stream.mapEffect(
                    // Each asset belongs to the stream's export span; per-asset spans would add churn.
                    Effect.fnUntraced(function* ({ attachment, inferSize }) {
                      const file = resolveAttachmentPath({
                        attachmentsDir: config.attachmentsDir,
                        attachment,
                      });
                      const bytes =
                        file === null
                          ? null
                          : yield* fs.readFile(file).pipe(
                              Effect.catchIf(
                                (error) => error.reason._tag === "NotFound",
                                () => Effect.succeed(null),
                              ),
                            );
                      sentAttachments.add(attachment.id);
                      return {
                        storeId,
                        afterSequence: cursor,
                        throughSequence: cursor,
                        anchorEventId,
                        events: [],
                        bootstrap,
                        complete: !bootstrap,
                        assets: [
                          {
                            attachment:
                              inferSize && bytes !== null
                                ? { ...attachment, sizeBytes: bytes.length }
                                : attachment,
                            data: bytes === null ? null : Buffer.from(bytes).toString("base64"),
                          },
                        ],
                      } satisfies HubArchiveBatch;
                    }),
                  ),
                );
                const events = Stream.fromEffect(
                  Effect.gen(function* () {
                    const next = group.at(-1)!.sequence;
                    const nextEventId = eventId(group.at(-1)!);
                    yield* rememberAnchor(next, nextEventId);
                    const batch = {
                      storeId,
                      afterSequence: cursor,
                      throughSequence: next,
                      anchorEventId: nextEventId,
                      events: group,
                      bootstrap,
                      complete: !bootstrap,
                    };
                    cursor = next;
                    anchorEventId = nextEventId;
                    return batch;
                  }),
                );
                return Stream.concat(assets, events);
              }),
            );
          // Retained events are the same replay source used by ProjectionMaintenance.rebuild after compaction.
          const bootstrap = batches(
            events.readApplicationEvents({ afterSequence, throughSequence: highWater }),
            true,
          );
          const complete = Stream.fromEffect(
            Effect.sync((): HubArchiveBatch => {
              const batch = {
                storeId,
                afterSequence: cursor,
                throughSequence: highWater,
                anchorEventId,
                events: [],
                bootstrap: true,
                complete: true,
              };
              cursor = highWater;
              return batch;
            }),
          );
          const live = batches(events.streamApplicationEvents({ afterSequence: highWater }), false);
          return Stream.concat(Stream.concat(bootstrap, complete), live);
        }),
      ).pipe(
        Stream.mapError((cause) =>
          isHubArchiveExportError(cause) ? cause : new HubArchiveExportError({ cause }),
        ),
      ),
  });
});

export const layer = Layer.effect(
  HubArchiveExporter,
  make.pipe(Effect.mapError((cause) => new HubArchiveExportError({ cause }))),
);
