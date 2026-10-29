import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
  type ChatAttachment,
  type ApplicationProjectEvent,
  type HubArchiveBatch,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as HubArchive from "./HubArchive.ts";
import * as HubArchiveExporter from "./HubArchiveExporter.ts";

const now = DateTime.makeUnsafe("2026-10-29T12:00:00.000Z");
const iso = DateTime.formatIso(now);
const projectId = ProjectId.make("hub-project");
const threadId = ThreadId.make("hub-thread");
const itemId = TurnItemId.make("hub-item");
const environmentId = EnvironmentId.make("hub-worker-a");
const otherEnvironmentId = EnvironmentId.make("hub-worker-b");

function projectEvent(): ApplicationProjectEvent {
  return {
    sequence: 1,
    eventId: EventId.make("hub-project-created"),
    aggregateKind: "project",
    aggregateId: projectId,
    type: "project.created",
    occurredAt: iso,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    payload: {
      projectId,
      title: "Hub project",
      workspaceRoot: "/workspace/hub",
      defaultModelSelection: null,
      scripts: [],
      createdAt: iso,
      updatedAt: iso,
    },
  };
}

function created(): OrchestrationV2DomainEvent {
  return {
    id: EventId.make("hub-thread-created"),
    threadId,
    type: "thread.created",
    occurredAt: now,
    payload: {
      id: threadId,
      projectId,
      title: "Stored session",
      providerInstanceId: ProviderInstanceId.make("codex"),
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "luna" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdBy: "user",
      creationSource: "web",
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  };
}

function commandEvent(index: number, output: string): OrchestrationV2DomainEvent {
  return {
    id: EventId.make(`hub-command-${index}`),
    threadId,
    type: "turn-item.updated",
    occurredAt: now,
    payload: {
      id: itemId,
      type: "command_execution",
      threadId,
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "completed",
      title: "Command",
      input: "echo output",
      output,
      exitCode: 0,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
    },
  };
}

function batch(output = "full stored output"): HubArchiveBatch {
  return {
    storeId: "source-a",
    afterSequence: 0,
    throughSequence: 5,
    bootstrap: true,
    complete: true,
    events: [
      projectEvent(),
      { sequence: 2, commandId: null, event: created() },
      { sequence: 5, commandId: null, event: commandEvent(1, output) },
    ],
    assets: [
      {
        attachment: {
          id: "hub-attachment",
          type: "file",
          name: "result.txt",
          mimeType: "text/plain",
          sizeBytes: 5,
        },
        data: "aGVsbG8=",
      },
    ],
  };
}

const withArchive = Effect.fn("HubArchiveTest.withArchive")(function* <A, E, R>(
  use: (archive: HubArchive.HubArchive["Service"]) => Effect.Effect<A, E, R>,
) {
  const context = yield* Layer.build(Layer.fresh(HubArchive.layer));
  return yield* use(Context.get(context, HubArchive.HubArchive));
}, Effect.scoped);

it.layer(NodeServices.layer)("HubArchive", (it) => {
  it.effect(
    "keeps offline sessions, full output and attachments after reopening, isolated by machine",
    () =>
      Effect.gen(function* () {
        const config = yield* Layer.build(
          ServerConfig.layerTest(process.cwd(), { prefix: "hub-archive-" }),
        );
        yield* withArchive((archive) =>
          Effect.gen(function* () {
            yield* archive.receiveBatch(environmentId, batch());
            yield* archive.receiveBatch(otherEnvironmentId, {
              ...batch("other machine output"),
              storeId: "source-b",
            });
            const shell = yield* archive.shell(environmentId);
            assert.equal(shell.projects[0]?.title, "Hub project");
            assert.equal(shell.threads[0]?.id, threadId);
          }),
        ).pipe(Effect.provide(config));
        yield* withArchive((archive) =>
          Effect.gen(function* () {
            yield* archive.receiveBatch(environmentId, {
              ...batch(),
              assets: batch().assets!.map((asset) => ({ ...asset, data: null })),
            });
            const saved = yield* archive.thread(environmentId, threadId);
            assert.equal(saved.projection.thread.title, "Stored session");
            const item = (yield* archive.item(environmentId, threadId, itemId)).item;
            assert.equal(
              item?.type === "command_execution" ? item.output : null,
              "full stored output",
            );
            const otherItem = (yield* archive.item(otherEnvironmentId, threadId, itemId)).item;
            assert.equal(
              otherItem?.type === "command_execution" ? otherItem.output : null,
              "other machine output",
            );
            const asset = yield* archive.asset(environmentId, "hub-attachment");
            assert.equal(Buffer.from(asset!.bytes).toString(), "hello");
            assert.equal((yield* archive.status(environmentId)).ackSequence, 5);
            assert.equal((yield* archive.status(environmentId)).missingAssets, 0);
            assert.equal((yield* archive.status(environmentId)).anchorEventId, "hub-command-1");
            assert.isNull(yield* archive.asset(environmentId, "missing"));
          }),
        ).pipe(Effect.provide(config));
      }),
  );

  it.effect(
    "replays batches idempotently and rejects gaps or changed sources without altering the archive",
    () =>
      Effect.gen(function* () {
        const config = yield* Layer.build(
          ServerConfig.layerTest(process.cwd(), { prefix: "hub-archive-cursors-" }),
        );
        yield* withArchive((archive) =>
          Effect.gen(function* () {
            const original = batch();
            yield* archive.receiveBatch(environmentId, original);
            assert.equal((yield* archive.receiveBatch(environmentId, original)).ackSequence, 5);
            for (const invalid of [
              { ...original, storeId: "replaced-source" },
              { ...original, afterSequence: 7, throughSequence: 8, events: [] },
              { ...original, events: [...original.events].reverse() },
              {
                ...original,
                afterSequence: 5,
                throughSequence: 7,
                events: [
                  { sequence: 6, commandId: null, event: commandEvent(8, "must roll back") },
                  { sequence: 7, commandId: null, event: created() },
                ],
              },
            ]) {
              const result = yield* archive
                .receiveBatch(environmentId, invalid)
                .pipe(Effect.result);
              assert.isTrue(Result.isFailure(result));
              assert.equal((yield* archive.status(environmentId)).ackSequence, 5);
              const retained = (yield* archive.item(environmentId, threadId, itemId)).item;
              assert.equal(
                retained?.type === "command_execution" ? retained.output : null,
                "full stored output",
              );
            }
            yield* archive.receiveBatch(environmentId, {
              storeId: original.storeId,
              afterSequence: 5,
              throughSequence: 12,
              events: [
                { sequence: 12, commandId: null, event: commandEvent(2, "incremental output") },
              ],
            });
            const item = (yield* archive.item(environmentId, threadId, itemId)).item;
            assert.equal(
              item?.type === "command_execution" ? item.output : null,
              "incremental output",
            );
            assert.equal((yield* archive.status(environmentId)).ackSequence, 12);
            const restored = yield* archive
              .receiveBatch(environmentId, original)
              .pipe(Effect.result);
            assert.isTrue(Result.isFailure(restored));
            assert.equal((yield* archive.status(environmentId)).ackSequence, 12);
          }),
        ).pipe(Effect.provide(config));
      }),
  );

  it.effect(
    "bootstraps a complete read replica from the retained log after source compaction",
    () =>
      Effect.gen(function* () {
        const config = yield* Layer.build(
          ServerConfig.layerTest(process.cwd(), { prefix: "hub-archive-bootstrap-" }),
        );
        const sourceBase = Layer.mergeAll(
          OrchestrationEventStore.layer,
          ProjectStore.layer,
          ProjectionStore.layer,
        ).pipe(Layer.provideMerge(Sqlite.layerMemory));
        const sourceEvents = EventStore.layerFromOrchestrationEventStore.pipe(
          Layer.provideMerge(sourceBase),
        );
        const sourceLayer = Layer.mergeAll(
          HubArchiveExporter.layer,
          ProjectionMaintenance.layer,
        ).pipe(Layer.provideMerge(sourceEvents));
        yield* Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const config = yield* ServerConfig.ServerConfig;
          const attachment: ChatAttachment = {
            id: "hub-source-attachment",
            type: "file",
            name: "result.txt",
            mimeType: "text/plain",
            sizeBytes: 5,
          };
          const missingAttachment = { ...attachment, id: "hub-missing-attachment" };
          const visualAttachment = {
            ...attachment,
            id: "hub-visual",
            name: "hub-visual.html",
            mimeType: "text/html",
          };
          yield* fs.writeFile(
            resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment })!,
            new TextEncoder().encode("hello"),
          );
          yield* fs.writeFile(
            resolveAttachmentPath({
              attachmentsDir: config.attachmentsDir,
              attachment: visualAttachment,
            })!,
            new TextEncoder().encode("<h1>Saved visual</h1>"),
          );
          const source = yield* Layer.build(sourceLayer);
          const events = Context.get(source, OrchestrationEventStore.OrchestrationEventStore);
          const projections = Context.get(source, ProjectionStore.ProjectionStoreV2);
          const projects = Context.get(source, ProjectStore.ProjectStoreV2);
          const { sequence: _sequence, ...unsequencedProject } = projectEvent();
          const project = yield* events.appendProjectEvent(unsequencedProject);
          yield* projects.apply(project);
          const stored = yield* events.appendAgentEvents({
            events: [
              created(),
              {
                id: EventId.make("hub-message"),
                threadId,
                type: "message.updated",
                occurredAt: now,
                payload: {
                  id: MessageId.make("hub-message"),
                  threadId,
                  runId: null,
                  nodeId: null,
                  role: "user",
                  text: "Keep my files",
                  attachments: [attachment, missingAttachment],
                  streaming: false,
                  createdBy: "user",
                  creationSource: "web",
                  createdAt: now,
                  updatedAt: now,
                },
              },
              {
                id: EventId.make("hub-visual"),
                threadId,
                type: "turn-item.updated",
                occurredAt: now,
                payload: {
                  id: TurnItemId.make("hub-visual"),
                  type: "dynamic_tool",
                  threadId,
                  runId: null,
                  nodeId: null,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 2,
                  status: "completed",
                  title: "Saved visual",
                  toolName: "t3-code.html_render",
                  input: {},
                  output: {
                    htmlRender: { attachmentId: visualAttachment.id, title: "Visual", height: 200 },
                  },
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                },
              },
              commandEvent(1, "first"),
              commandEvent(2, "superseded"),
              commandEvent(3, "complete output"),
            ],
          });
          for (const storedEvent of stored) yield* projections.apply(storedEvent.event);
          const compacted = yield* Context.get(
            source,
            ProjectionMaintenance.ProjectionMaintenanceV2,
          ).compactEventStore();
          assert.isAbove(compacted.deletedEventCount, 0);
          const expected = yield* projections.getThreadProjection(threadId);
          const batches = yield* Context.get(source, HubArchiveExporter.HubArchiveExporter)
            .stream({})
            .pipe(
              Stream.takeUntil((entry) => entry.complete === true),
              Stream.runCollect,
            );
          assert.isTrue(batches.at(-1)!.complete);
          yield* withArchive((archive) =>
            Effect.gen(function* () {
              for (const entry of batches) yield* archive.receiveBatch(environmentId, entry);
              const detail = (yield* archive.item(environmentId, threadId, itemId)).item;
              assert.deepEqual(
                detail,
                expected.turnItems.find((item) => item.id === itemId),
              );
              const status = yield* archive.status(environmentId);
              assert.isTrue(status.complete);
              assert.equal(status.missingAssets, 1);
              assert.equal(status.ackSequence, yield* events.latestApplicationSequence);
              assert.equal(
                Buffer.from((yield* archive.asset(environmentId, attachment.id))!.bytes).toString(),
                "hello",
              );
              assert.equal(
                Buffer.from(
                  (yield* archive.asset(environmentId, visualAttachment.id))!.bytes,
                ).toString(),
                "<h1>Saved visual</h1>",
              );
              assert.isNull(yield* archive.asset(environmentId, missingAttachment.id));
              const shell = yield* archive.shell(environmentId);
              assert.equal(shell.threads.length, 1);
              const exporter = Context.get(source, HubArchiveExporter.HubArchiveExporter);
              yield* events.appendAgentEvents({ events: [commandEvent(4, "next stored output")] });
              yield* Context.get(
                source,
                ProjectionMaintenance.ProjectionMaintenanceV2,
              ).compactEventStore();
              const sql = Context.get(source, SqlClient.SqlClient);
              const removedAnchor = yield* sql`SELECT event_id FROM orchestration_events
                WHERE sequence = ${status.ackSequence}`;
              assert.equal(removedAnchor.length, 0);
              const resumed = yield* exporter
                .stream({
                  afterSequence: status.ackSequence,
                  afterEventId: status.anchorEventId,
                })
                .pipe(
                  Stream.takeUntil((entry) => entry.bootstrap === true && entry.complete === true),
                  Stream.runCollect,
                );
              for (const entry of resumed) yield* archive.receiveBatch(environmentId, entry);
              const advanced = yield* archive.status(environmentId);
              assert.equal(advanced.anchorEventId, "hub-command-4");
              yield* sql`DELETE FROM orchestration_events WHERE sequence > ${status.ackSequence}`;
              yield* sql`DELETE FROM hub_replication_anchors WHERE sequence > ${status.ackSequence}`;
              yield* sql`UPDATE sqlite_sequence SET seq = ${status.ackSequence}
                WHERE name = 'orchestration_events'`;
              yield* events.appendAgentEvents({
                events: [commandEvent(5, "divergent output"), commandEvent(6, "newer divergence")],
              });
              assert.isAbove(yield* events.latestApplicationSequence, advanced.ackSequence);
              const divergent = yield* exporter
                .stream({
                  afterSequence: advanced.ackSequence,
                  afterEventId: advanced.anchorEventId,
                })
                .pipe(
                  Stream.takeUntil((entry) => entry.bootstrap === true && entry.complete === true),
                  Stream.runCollect,
                  Effect.result,
                );
              assert.isTrue(Result.isFailure(divergent));
              const retained = (yield* archive.item(environmentId, threadId, itemId)).item;
              assert.equal(
                retained?.type === "command_execution" ? retained.output : null,
                "next stored output",
              );
              yield* archive.reportError(environmentId, "source-changed");
              assert.include(
                (yield* archive.status(environmentId)).syncError!,
                "syncing has stopped",
              );
            }),
          );
        }).pipe(Effect.provide(config));
      }),
  );
});
