import { randomUUID } from "~/lib/utils";
import { useAtomValue } from "@effect/atom-react";
import {
  applyCommandOutputFrame,
  EMPTY_COMMAND_OUTPUT,
} from "@t3tools/client-runtime/state/command-output";
import {
  type EnvironmentId,
  type HubConfigurationIntent,
  type HubEnrollmentResult,
  type HubEnvironmentSummary,
  type OrchestrationV2ThreadHistoryPage,
  type ThreadId,
} from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { useState } from "react";
import { useEnvironments } from "~/state/environments";
import { hubEnvironment } from "~/state/hub";
import { useEnvironmentQuery } from "~/state/query";
import { useAtomCommand } from "~/state/use-atom-command";
import { SettingsSection } from "../settings/settingsLayout";
import { useOptionalSettingsScope } from "../settings/SettingsScopeContext";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { HubConfigurationEditor } from "./HubConfigurationEditor";
import { HubSessionTimeline } from "./HubSessionTimeline";
import type { TurnItemSource } from "../chat/timelineRowContext";

export function HubSettings() {
  const { environments } = useEnvironments();
  const settings = useOptionalSettingsScope();
  const [selectedHub, setSelectedHub] = useState<EnvironmentId | null>(null);
  const connected = environments.filter(
    (environment) =>
      environment.connection.phase === "connected" &&
      environment.entry.target._tag !== "HubConnectionTarget",
  );
  const scopedEnvironmentId =
    settings?.scope.kind === "environment" ||
    settings?.scope.kind === "checkout" ||
    settings?.scope.kind === "project"
      ? settings.scope.environmentId
      : null;
  const scopedEnvironment = environments.find(
    (environment) => environment.environmentId === scopedEnvironmentId,
  );
  const scopedHubId =
    scopedEnvironment?.entry.target._tag === "HubConnectionTarget"
      ? scopedEnvironment.entry.target.hubEnvironmentId
      : scopedEnvironment?.environmentId;
  const referencedHubs = new Set(
    environments.flatMap((environment) =>
      environment.entry.target._tag === "HubConnectionTarget"
        ? [environment.entry.target.hubEnvironmentId]
        : [],
    ),
  );
  const defaultHub =
    connected.find((environment) => environment.environmentId === scopedHubId) ??
    connected.find((environment) => referencedHubs.has(environment.environmentId)) ??
    connected.find((environment) => environment.entry.target._tag !== "PrimaryConnectionTarget") ??
    connected[0];
  const hub =
    selectedHub === null
      ? defaultHub
      : connected.find((environment) => environment.environmentId === selectedHub);
  const [expanded, setExpanded] = useState(false);
  return (
    <SettingsSection
      id="home-server"
      title="Home server"
      headerAction={
        <Button variant="outline" size="sm" onClick={() => setExpanded(!expanded)}>
          {expanded ? "Close" : "Manage hub"}
        </Button>
      }
    >
      <div className="space-y-4 p-4">
        <p className="text-sm text-muted-foreground">
          Keep sessions from your machines on one self-hosted server. Browse saved sessions and
          configure machines while they are offline.
        </p>
        {expanded ? (
          <>
            <Select
              value={hub?.environmentId ?? null}
              onValueChange={(value) => {
                if (value) setSelectedHub(value);
              }}
            >
              <SelectTrigger aria-label="Hub server">
                <SelectValue placeholder="Choose a connected server" />
              </SelectTrigger>
              <SelectPopup>
                {connected.map((environment) => (
                  <SelectItem key={environment.environmentId} value={environment.environmentId}>
                    {environment.label}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
            {hub ? (
              <HubManagement key={hub.environmentId} hubId={hub.environmentId} />
            ) : (
              <p className="text-sm text-muted-foreground">
                Connect to your home server in Environments to manage its machines.
              </p>
            )}
          </>
        ) : null}
      </div>
    </SettingsSection>
  );
}

function HubManagement({ hubId }: { readonly hubId: EnvironmentId }) {
  const inventory = useEnvironmentQuery(
    hubEnvironment.inventory({ environmentId: hubId, input: {} }),
  );
  const createEnrollment = useAtomCommand(hubEnvironment.createEnrollment);
  const canEnroll = useAtomValue(hubEnvironment.createEnrollment.permissionAtom(hubId));
  const [label, setLabel] = useState("");
  const [enrollment, setEnrollment] = useState<HubEnrollmentResult | null>(null);
  const [selected, setSelected] = useState<EnvironmentId | null>(null);
  const [busy, setBusy] = useState(false);
  async function enroll() {
    setBusy(true);
    const result = await createEnrollment({
      environmentId: hubId,
      input: label.trim() ? { label: label.trim() } : {},
    });
    setBusy(false);
    if (result._tag === "Success") setEnrollment(result.value);
  }
  const machine = inventory.data?.environments.find((entry) => entry.environmentId === selected);
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <div className="min-w-40 flex-1">
          <Input
            aria-label="New machine name"
            placeholder="Machine name (optional)"
            value={label}
            onChange={(event) => setLabel(event.target.value)}
          />
        </div>
        <Button disabled={busy || !canEnroll} onClick={() => void enroll()}>
          Pair machine
        </Button>
        <Button variant="ghost" onClick={inventory.refresh}>
          Refresh
        </Button>
      </div>
      {enrollment ? (
        <div className="space-y-2 rounded-lg border p-3 text-sm">
          <p>
            On the machine, run{" "}
            <code>t3 hub join &lt;your-hub-url&gt; --code {enrollment.code}</code>
          </p>
          <p className="text-xs text-muted-foreground">
            Pairing expires {new Date(enrollment.expiresAt).toLocaleString()}. Keep this code
            private.
          </p>
          <Button variant="ghost" size="sm" onClick={() => setEnrollment(null)}>
            Hide pairing code
          </Button>
        </div>
      ) : null}
      {inventory.error ? (
        <p role="alert" className="text-sm text-destructive">
          {inventory.error}
        </p>
      ) : null}
      {inventory.isPending ? (
        <p className="text-sm text-muted-foreground">Loading machines…</p>
      ) : null}
      {inventory.data?.environments.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          Pair a machine to start saving its sessions here.
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {inventory.data?.environments.map((entry) => (
          <Button
            key={entry.environmentId}
            variant={selected === entry.environmentId ? "secondary" : "outline"}
            onClick={() => setSelected(entry.environmentId)}
          >
            {entry.label} · {entry.online ? "Online" : "Offline"}
          </Button>
        ))}
      </div>
      {machine ? (
        <HubMachine
          key={machine.environmentId}
          hubId={hubId}
          machine={machine}
          onRemoved={() => {
            setSelected(null);
            inventory.refresh();
          }}
        />
      ) : null}
    </div>
  );
}

function HubMachine({
  hubId,
  machine,
  onRemoved,
}: {
  readonly hubId: EnvironmentId;
  readonly machine: HubEnvironmentSummary;
  readonly onRemoved: () => void;
}) {
  const target = { environmentId: hubId, input: { environmentId: machine.environmentId } };
  const shell = useEnvironmentQuery(hubEnvironment.archiveShell(target));
  const archiveStatus = useEnvironmentQuery(hubEnvironment.archiveStatus(target));
  const configuration = useEnvironmentQuery(hubEnvironment.configuration(target));
  const observed = useEnvironmentQuery(hubEnvironment.observedConfiguration(target));
  const enqueue = useAtomCommand(hubEnvironment.enqueueConfiguration);
  const cancelConfiguration = useAtomCommand(hubEnvironment.cancelConfiguration);
  const retryConfiguration = useAtomCommand(hubEnvironment.retryConfiguration);
  const remove = useAtomCommand(hubEnvironment.removeEnvironment);
  const canConfigure = useAtomValue(hubEnvironment.enqueueConfiguration.permissionAtom(hubId));
  const canRemove = useAtomValue(hubEnvironment.removeEnvironment.permissionAtom(hubId));
  const [threadId, setThreadId] = useState<ThreadId | null>(null);
  const [removing, setRemoving] = useState(false);
  const [confirmRemoval, setConfirmRemoval] = useState(false);
  async function save(intent: HubConfigurationIntent) {
    if (!configuration.data) return false;
    const hashKey =
      intent.type === "settings.patch" || intent.type === "provider.mutate"
        ? "settings"
        : intent.type === "project.update" || intent.type === "project.delete"
          ? `project:${intent.projectId}`
          : intent.type === "automation.delete" ||
              intent.type === "automation.setEnabled" ||
              intent.type === "automation.upsert"
            ? `automation:${intent.input.id ?? ""}`
            : null;
    const expectedHash = hashKey === null ? undefined : observed.data?.hashes[hashKey];
    const result = await enqueue({
      environmentId: hubId,
      input: {
        environmentId: machine.environmentId,
        expectedRevision: configuration.data.revision,
        requestId: randomUUID(),
        intent,
        ...(expectedHash ? { expectedHash } : {}),
      },
    });
    configuration.refresh();
    observed.refresh();
    return result._tag === "Success";
  }
  async function removeMachine() {
    setRemoving(true);
    const result = await remove(target);
    setRemoving(false);
    if (result._tag === "Success") onRemoved();
  }
  const threads = shell.data ? [...shell.data.threads, ...shell.data.archivedThreads] : [];
  return (
    <div className="space-y-5 border-t pt-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="text-sm font-medium">{machine.label}</h3>
          <p className="text-xs text-muted-foreground">
            {machine.online
              ? "Connected to your hub"
              : machine.lastSeenAt
                ? `Last connected ${new Date(machine.lastSeenAt).toLocaleString()}`
                : "Waiting for first connection"}
          </p>
        </div>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => {
            shell.refresh();
            archiveStatus.refresh();
            configuration.refresh();
          }}
        >
          Refresh machine
        </Button>
      </div>
      <div className="space-y-2">
        <h4 className="text-sm font-medium">Saved sessions</h4>
        {archiveStatus.data?.syncError ? (
          <p role="alert" className="text-sm text-destructive">
            Session sync failed: {archiveStatus.data.syncError}
          </p>
        ) : null}
        {archiveStatus.data?.lastSyncedAt ? (
          <p className="text-xs text-muted-foreground">
            Last synced {new Date(archiveStatus.data.lastSyncedAt).toLocaleString()}
            {archiveStatus.data.complete ? "" : " · Initial sync in progress"}
          </p>
        ) : null}
        {shell.error ? (
          <p role="alert" className="text-sm text-destructive">
            {shell.error}
          </p>
        ) : null}
        {shell.data && threads.length === 0 ? (
          <p className="text-sm text-muted-foreground">No sessions have synced yet.</p>
        ) : null}
        <div className="flex flex-wrap gap-2">
          {threads.map((thread) => (
            <Button
              key={thread.id}
              variant={threadId === thread.id ? "secondary" : "outline"}
              size="sm"
              onClick={() => setThreadId(thread.id)}
            >
              {thread.title}
            </Button>
          ))}
        </div>
        {threadId ? (
          <HubSession
            key={threadId}
            hubId={hubId}
            workerId={machine.environmentId}
            threadId={threadId}
          />
        ) : null}
      </div>
      {configuration.error ? (
        <p role="alert" className="text-sm text-destructive">
          {configuration.error}
        </p>
      ) : null}
      {configuration.data && canConfigure ? (
        <HubConfigurationEditor
          state={configuration.data}
          observed={observed.data}
          onSubmit={save}
          onCancel={async (requestId) => {
            await cancelConfiguration({ ...target, input: { ...target.input, requestId } });
            configuration.refresh();
          }}
          onRetry={async () => {
            await retryConfiguration(target);
            configuration.refresh();
          }}
        />
      ) : null}
      <div className="space-y-2 border-t pt-3">
        {confirmRemoval ? (
          <>
            <p className="text-sm">Remove {machine.label} from this hub and revoke its access?</p>
            <div className="flex gap-2">
              <Button
                variant="destructive"
                disabled={removing}
                onClick={() => void removeMachine()}
              >
                Remove machine
              </Button>
              <Button variant="outline" onClick={() => setConfirmRemoval(false)}>
                Cancel
              </Button>
            </div>
          </>
        ) : (
          <Button
            variant="ghost"
            size="sm"
            disabled={!canRemove}
            onClick={() => setConfirmRemoval(true)}
          >
            Remove machine…
          </Button>
        )}
      </div>
    </div>
  );
}

function HubSession({
  hubId,
  workerId,
  threadId,
}: {
  readonly hubId: EnvironmentId;
  readonly workerId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const target = { environmentId: hubId, input: { environmentId: workerId, threadId } };
  const thread = useEnvironmentQuery(hubEnvironment.archiveThread(target));
  const readAttachment = useAtomCommand(hubEnvironment.archiveAttachment);
  const [downloading, setDownloading] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const history = useEnvironmentQuery(
    cursor === null
      ? null
      : hubEnvironment.archiveHistory({ ...target, input: { ...target.input, cursor } }),
  );
  const [pages, setPages] = useState<readonly OrchestrationV2ThreadHistoryPage[]>([]);
  const [previousPage, setPreviousPage] = useState<OrchestrationV2ThreadHistoryPage | null>(null);
  if (history.data && history.data !== previousPage) {
    setPreviousPage(history.data);
    setPages([...pages, history.data]);
  }
  const snapshot = thread.data?.kind === "snapshot" ? thread.data : null;
  const latestPage = pages.at(-1);
  const nextCursor = latestPage ? latestPage.nextCursor : snapshot?.historyCursor;
  const hasMoreHistory = latestPage ? latestPage.hasMoreHistory : snapshot?.hasMoreHistory === true;
  const projection = snapshot
    ? {
        ...snapshot.projection,
        visibleTurnItems: [
          ...pages.toReversed().flatMap((page) => page.items),
          ...snapshot.projection.visibleTurnItems,
        ],
      }
    : null;
  const source: TurnItemSource = {
    detail: ({ itemId }) =>
      hubEnvironment.archiveTurnItem({ ...target, input: { ...target.input, turnItemId: itemId } }),
    commandOutput: ({ itemId }) =>
      Atom.map(
        hubEnvironment.archiveTurnItem({
          ...target,
          input: { ...target.input, turnItemId: itemId },
        }),
        (result) =>
          AsyncResult.map(result, ({ item }) =>
            applyCommandOutputFrame(EMPTY_COMMAND_OUTPUT, {
              kind: "replace",
              text: item?.type === "command_execution" ? (item.output ?? "") : "",
              truncated: false,
              running: false,
            }),
          ),
      ),
  };
  const attachments = new Map(
    projection?.visibleTurnItems.flatMap(({ item }) =>
      item.type === "user_message" || item.type === "assistant_message"
        ? (item.attachments ?? [])
            .filter((attachment) => attachment.type === "image" || attachment.type === "file")
            .map((attachment) => [attachment.id, attachment] as const)
        : [],
    ),
  );
  async function downloadAttachment(attachment: {
    readonly id: string;
    readonly name: string;
    readonly mimeType: string;
  }) {
    setDownloading(attachment.id);
    const result = await readAttachment({
      environmentId: hubId,
      input: { environmentId: workerId, attachmentId: attachment.id },
    });
    setDownloading(null);
    if (result._tag !== "Success") return;
    const url = URL.createObjectURL(
      new Blob([new Uint8Array(result.value)], { type: attachment.mimeType }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = attachment.name;
    link.click();
    URL.revokeObjectURL(url);
  }
  return (
    <div className="space-y-2">
      {thread.error ? (
        <p role="alert" className="text-sm text-destructive">
          {thread.error}
        </p>
      ) : null}
      {thread.isPending ? (
        <p className="text-sm text-muted-foreground">Loading saved session…</p>
      ) : null}
      {projection ? (
        <>
          <p className="text-xs text-muted-foreground">
            Saved on your hub. Changes appear after the machine syncs.
          </p>
          <HubSessionTimeline
            projection={projection}
            turnItemSource={source}
            historyControls={{
              hasMoreHistory,
              loading: history.isPending,
              error: history.error,
              onLoadEarlier: () => {
                if (nextCursor) {
                  if (nextCursor === cursor) history.refresh();
                  else setCursor(nextCursor);
                }
              },
            }}
            onDownloadVisual={(attachmentId, title) =>
              void downloadAttachment({
                id: attachmentId,
                name: `${title}.html`,
                mimeType: "text/html",
              })
            }
          />
          {attachments.size > 0 ? (
            <div className="space-y-2">
              <h4 className="text-sm font-medium">Saved attachments</h4>
              <div className="flex flex-wrap gap-2">
                {[...attachments.values()].map((attachment) => (
                  <Button
                    key={attachment.id}
                    variant="outline"
                    size="sm"
                    disabled={downloading !== null}
                    onClick={() => void downloadAttachment(attachment)}
                  >
                    {downloading === attachment.id ? "Downloading…" : attachment.name}
                  </Button>
                ))}
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </div>
  );
}
