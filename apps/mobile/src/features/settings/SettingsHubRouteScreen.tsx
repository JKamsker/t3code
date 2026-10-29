import { useAtomValue } from "@effect/atom-react";
import { useNavigation } from "@react-navigation/native";
import {
  CommandId,
  HubConfigurationIntent,
  ProjectId,
  ScheduledTaskId,
  type EnvironmentId,
  type ChatAttachment,
  type HubEnrollmentResult,
  type HubObservedConfiguration,
  type ThreadId,
  type TurnItemId,
} from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import * as Schema from "effect/Schema";
import { useRef, useState } from "react";
import { View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppText as Text, AppTextInput } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import * as Dialog from "../../components/ui/Dialog";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { shareGeneratedAttachment } from "../../lib/attachmentDownload";
import { uuidv4 } from "../../lib/uuid";
import { useEnvironments } from "../../state/environments";
import { hubEnvironment } from "../../state/hub";
import { useEnvironmentQuery } from "../../state/query";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsRow } from "./components/SettingsRow";
import { SettingsScreen } from "./components/SettingsScreen";
import { SettingsSection } from "./components/SettingsSection";

const decodeConfigurationIntent = Schema.decodeUnknownSync(HubConfigurationIntent);

export function SettingsHubRouteScreen() {
  const { environments } = useEnvironments();
  const [selectedHub, setSelectedHub] = useState<EnvironmentId | null>(null);
  const connected = environments.filter(
    (environment) => environment.connection.phase === "connected",
  );
  const hub =
    connected.find((environment) => environment.environmentId === selectedHub) ?? connected[0];
  const navigation = useNavigation();
  const insets = useSafeAreaInsets();
  return (
    <SettingsScreen title="Hub">
      <ScreenScrollView
        contentInsetAdjustmentBehavior="automatic"
        keyboardShouldPersistTaps="handled"
        contentContainerClassName="gap-5 px-5 pt-4"
        contentContainerStyle={{ paddingBottom: insets.bottom + 24 }}
      >
        <Text className="text-sm text-foreground-muted">
          Your own server keeps sessions and connects your machines through one address. Pair this
          device with your hub, then enroll your machines. No cloud account is required.
        </Text>
        <SettingsSection title="Hub server">
          {connected.map((environment) => (
            <SettingsRow
              key={environment.environmentId}
              icon="server.rack"
              label={environment.label}
              value={hub?.environmentId === environment.environmentId ? "Selected" : undefined}
              onPress={() => setSelectedHub(environment.environmentId)}
            />
          ))}
          <SettingsRow
            icon="plus"
            label="Pair a hub"
            onPress={() => navigation.navigate("SettingsEnvironmentNew")}
          />
        </SettingsSection>
        {hub ? <HubManagement key={hub.environmentId} hubId={hub.environmentId} /> : null}
      </ScreenScrollView>
    </SettingsScreen>
  );
}

function HubManagement({ hubId }: { readonly hubId: EnvironmentId }) {
  const inventory = useEnvironmentQuery(
    hubEnvironment.inventory({ environmentId: hubId, input: {} }),
  );
  const createEnrollment = useAtomCommand(hubEnvironment.createEnrollment);
  const removeEnvironment = useAtomCommand(hubEnvironment.removeEnvironment);
  const canManage = useAtomValue(hubEnvironment.createEnrollment.permissionAtom(hubId));
  const [enrollment, setEnrollment] = useState<HubEnrollmentResult | null>(null);
  const [selectedMachine, setSelectedMachine] = useState<EnvironmentId | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function enroll() {
    setBusy(true);
    setError(null);
    const result = await createEnrollment({ environmentId: hubId, input: {} });
    setBusy(false);
    if (result._tag === "Success") setEnrollment(result.value);
    else setError(String(squashAtomCommandFailure(result)));
  }

  return (
    <>
      {inventory.error ? <Text className="text-danger">{inventory.error}</Text> : null}
      {error ? <Text className="text-danger">{error}</Text> : null}
      <SettingsSection
        title="Machines"
        footer="An offline machine keeps its last synced sessions available. Queued configuration takes effect after it reconnects."
      >
        {inventory.data?.environments.length === 0 ? (
          <Text className="p-4 text-foreground-muted">No machines enrolled yet.</Text>
        ) : null}
        {inventory.data?.environments.map((machine) => (
          <View key={machine.environmentId} className="gap-3 p-4">
            <SettingsRow
              icon="desktopcomputer"
              label={machine.label}
              value={`${machine.online ? "Online" : "Offline"}${machine.lastSeenAt ? ` · Last seen ${new Date(machine.lastSeenAt).toLocaleString()}` : ""}`}
              valuePosition="below"
              onPress={() => setSelectedMachine(machine.environmentId)}
            />
            {selectedMachine === machine.environmentId ? (
              <>
                <MachineDetails
                  hubId={hubId}
                  machineId={machine.environmentId}
                  online={machine.online}
                />
                <Button
                  label="Remove machine"
                  tone="danger"
                  disabled={!canManage || busy}
                  onPress={() =>
                    Dialog.confirm({
                      title: `Remove ${machine.label}?`,
                      message:
                        "This revokes its hub connection. Pair the machine again to reconnect it.",
                      confirmText: "Remove",
                      destructive: true,
                      onConfirm: () => {
                        void (async () => {
                          setBusy(true);
                          const result = await removeEnvironment({
                            environmentId: hubId,
                            input: { environmentId: machine.environmentId },
                          });
                          setBusy(false);
                          if (result._tag === "Success") {
                            setSelectedMachine(null);
                            inventory.refresh();
                          } else setError(String(squashAtomCommandFailure(result)));
                        })();
                      },
                    })
                  }
                />
              </>
            ) : null}
          </View>
        ))}
      </SettingsSection>
      <View className="flex-row flex-wrap gap-3">
        <Button
          label="Refresh machines"
          disabled={inventory.isPending}
          onPress={inventory.refresh}
        />
        <Button
          label="Enroll machine"
          disabled={!canManage}
          loading={busy}
          onPress={() => void enroll()}
        />
      </View>
      {enrollment ? (
        <SettingsSection
          title="Enrollment code"
          footer={`Expires ${new Date(enrollment.expiresAt).toLocaleString()}. Run t3 hub join on the machine with your hub address and this code.`}
        >
          <Text selectable className="p-4 font-mono text-lg">
            {enrollment.code}
          </Text>
          <Button label="Dismiss code" tone="text" onPress={() => setEnrollment(null)} />
        </SettingsSection>
      ) : null}
    </>
  );
}

function MachineDetails({
  hubId,
  machineId,
  online,
}: {
  readonly hubId: EnvironmentId;
  readonly machineId: EnvironmentId;
  readonly online: boolean;
}) {
  const configuration = useEnvironmentQuery(
    hubEnvironment.configuration({ environmentId: hubId, input: { environmentId: machineId } }),
  );
  const cancel = useAtomCommand(hubEnvironment.cancelConfiguration);
  const retry = useAtomCommand(hubEnvironment.retryConfiguration);
  const canManage = useAtomValue(hubEnvironment.cancelConfiguration.permissionAtom(hubId));
  const observed = useEnvironmentQuery(
    hubEnvironment.observedConfiguration({
      environmentId: hubId,
      input: { environmentId: machineId },
    }),
  );
  const shell = useEnvironmentQuery(
    hubEnvironment.archiveShell({ environmentId: hubId, input: { environmentId: machineId } }),
  );
  const archiveStatus = useEnvironmentQuery(
    hubEnvironment.archiveStatus({ environmentId: hubId, input: { environmentId: machineId } }),
  );
  const [actionError, setActionError] = useState<string | null>(null);
  const [threadId, setThreadId] = useState<ThreadId | null>(null);
  return (
    <View className="gap-4">
      <Text className="font-t3-medium">Saved sessions</Text>
      {archiveStatus.data ? (
        <Text className="text-sm text-foreground-muted">
          {archiveStatus.data.syncError
            ? `Session sync failed: ${archiveStatus.data.syncError}`
            : archiveStatus.data.complete
              ? "Session history synced"
              : "Session history syncing"}
          {archiveStatus.data.lastSyncedAt
            ? ` · ${new Date(archiveStatus.data.lastSyncedAt).toLocaleString()}`
            : ""}
          {archiveStatus.data.missingAssets
            ? ` · ${archiveStatus.data.missingAssets} attachments ${archiveStatus.data.syncError ? "unavailable" : "still syncing"}`
            : ""}
        </Text>
      ) : null}
      {actionError ? <Text className="text-danger">{actionError}</Text> : null}
      {!online ? (
        <Text className="text-sm text-foreground-muted">
          Machine offline. Sessions show the last synced state.
        </Text>
      ) : null}
      {shell.error ? <Text className="text-danger">{shell.error}</Text> : null}
      {shell.data
        ? [...shell.data.threads, ...shell.data.archivedThreads].map((thread) => (
            <SettingsRow
              key={thread.id}
              icon="text.bubble"
              label={thread.title || "Untitled session"}
              onPress={() => setThreadId(thread.id)}
            />
          ))
        : null}
      {threadId ? (
        <ArchivedSession key={threadId} hubId={hubId} machineId={machineId} threadId={threadId} />
      ) : null}
      <Text className="font-t3-medium">Configuration</Text>
      {configuration.error ? <Text className="text-danger">{configuration.error}</Text> : null}
      {configuration.data ? (
        <ConfigurationEditor
          hubId={hubId}
          machineId={machineId}
          revision={configuration.data.revision}
          observed={observed.data}
          onSaved={configuration.refresh}
        />
      ) : null}
      {configuration.data?.entries.map((entry) => (
        <View key={entry.requestId} className="gap-1">
          <Text>
            {entry.type} · {entry.status}
          </Text>
          {entry.error ? <Text className="text-danger">{entry.error}</Text> : null}
          {entry.status === "pending" ? (
            <Button
              label="Cancel change"
              tone="text"
              disabled={!canManage}
              onPress={() => {
                void cancel({
                  environmentId: hubId,
                  input: { environmentId: machineId, requestId: entry.requestId },
                }).then((result) => {
                  if (result._tag !== "Success")
                    setActionError(String(squashAtomCommandFailure(result)));
                  configuration.refresh();
                });
              }}
            />
          ) : null}
        </View>
      ))}
      {configuration.data?.entries.some((entry) => entry.status === "failed") ? (
        <Button
          label="Retry failed changes"
          disabled={!canManage}
          onPress={() => {
            void retry({ environmentId: hubId, input: { environmentId: machineId } }).then(
              (result) => {
                if (result._tag !== "Success")
                  setActionError(String(squashAtomCommandFailure(result)));
                configuration.refresh();
              },
            );
          }}
        />
      ) : null}
      <Button
        label="Refresh saved state"
        tone="text"
        onPress={() => {
          configuration.refresh();
          shell.refresh();
          observed.refresh();
          archiveStatus.refresh();
        }}
      />
    </View>
  );
}

function ConfigurationEditor({
  hubId,
  machineId,
  revision,
  observed,
  onSaved,
}: {
  readonly hubId: EnvironmentId;
  readonly machineId: EnvironmentId;
  readonly revision: number;
  readonly observed: HubObservedConfiguration | null;
  readonly onSaved: () => void;
}) {
  const enqueue = useAtomCommand(hubEnvironment.enqueueConfiguration);
  const canConfigure = useAtomValue(hubEnvironment.enqueueConfiguration.permissionAtom(hubId));
  const [editingProject, setEditingProject] = useState<ProjectId | null>(null);
  const [projectTitle, setProjectTitle] = useState("");
  const [projectPath, setProjectPath] = useState("");
  const [projectIdentity, setProjectIdentity] = useState(() => ({
    commandId: CommandId.make(uuidv4()),
    projectId: ProjectId.make(uuidv4()),
  }));
  const [showSettings, setShowSettings] = useState(false);
  const [automationId, setAutomationId] = useState("");
  const pendingRequest = useRef<{ readonly serialized: string; readonly requestId: string } | null>(
    null,
  );
  const [draft, setDraft] = useState('{\n  "type": "settings.patch",\n  "patch": {}\n}');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  async function queue(intent: HubConfigurationIntent) {
    setBusy(true);
    setMessage(null);
    const hashKey =
      "projectId" in intent
        ? `project:${intent.projectId}`
        : "input" in intent
          ? `automation:${intent.input.id ?? ""}`
          : "settings";
    const expectedHash = observed?.hashes[hashKey];
    const serialized = JSON.stringify(intent);
    if (pendingRequest.current?.serialized !== serialized)
      pendingRequest.current = { serialized, requestId: uuidv4() };
    const result = await enqueue({
      environmentId: hubId,
      input: {
        environmentId: machineId,
        expectedRevision: revision,
        ...(expectedHash ? { expectedHash } : {}),
        requestId: pendingRequest.current.requestId,
        intent,
      },
    });
    setBusy(false);
    if (result._tag === "Success") {
      pendingRequest.current = null;
      setMessage("Change queued. It will apply when the machine is connected.");
      onSaved();
      return true;
    } else setMessage(String(squashAtomCommandFailure(result)));
    return false;
  }
  return (
    <View className="gap-3">
      <Text className="text-sm text-foreground-muted">
        Queue changes even while this machine is offline. Settings, providers, projects and
        automations are supported.
      </Text>
      {observed ? (
        <>
          <Text className="font-t3-medium">Machine settings</Text>
          <Button
            label={`Queue ${observed.settings.autoResumeLimitedThreads ? "disable" : "enable"} automatic resume`}
            disabled={!canConfigure}
            loading={busy}
            onPress={() =>
              void queue({
                type: "settings.patch",
                patch: { autoResumeLimitedThreads: !observed.settings.autoResumeLimitedThreads },
              })
            }
          />
          <Button
            label={`Queue ${observed.settings.enableDeviceSupport ? "disable" : "enable"} device support`}
            disabled={!canConfigure}
            loading={busy}
            onPress={() =>
              void queue({
                type: "settings.patch",
                patch: { enableDeviceSupport: !observed.settings.enableDeviceSupport },
              })
            }
          />
          <Button
            label={showSettings ? "Hide reported settings" : "View reported settings"}
            tone="text"
            onPress={() => setShowSettings(!showSettings)}
          />
          {showSettings ? (
            <Text selectable className="rounded-xl bg-subtle p-3 font-mono text-xs">
              {JSON.stringify(observed.settings, null, 2)}
            </Text>
          ) : null}
          <Text className="font-t3-medium">Projects</Text>
          {observed.projects.projects.map((project) => (
            <SettingsRow
              key={project.id}
              icon="folder"
              label={project.title}
              value={project.workspaceRoot}
              valuePosition="below"
              onPress={() => {
                setEditingProject(project.id);
                setProjectTitle(project.title);
                setProjectPath(project.workspaceRoot);
              }}
            />
          ))}
        </>
      ) : null}
      <HubField label="Project name" value={projectTitle} onChangeText={setProjectTitle} />
      <HubField
        label="Project directory on machine"
        value={projectPath}
        onChangeText={setProjectPath}
      />
      <Button
        label={editingProject ? "Queue project update" : "Queue new project"}
        disabled={!canConfigure || !projectTitle.trim() || !projectPath.trim()}
        loading={busy}
        onPress={() =>
          void queue({
            type: editingProject ? "project.update" : "project.create",
            commandId: projectIdentity.commandId,
            projectId: editingProject ?? projectIdentity.projectId,
            title: projectTitle,
            workspaceRoot: projectPath,
          }).then((saved) => {
            if (saved)
              setProjectIdentity({
                commandId: CommandId.make(uuidv4()),
                projectId: ProjectId.make(uuidv4()),
              });
          })
        }
      />
      {editingProject ? (
        <Button
          label="New project"
          tone="text"
          onPress={() => {
            setEditingProject(null);
            setProjectTitle("");
            setProjectPath("");
          }}
        />
      ) : null}
      <Text className="font-t3-medium">Automation</Text>
      {observed?.automation.tasks.map((task) => (
        <View key={task.id} className="gap-2">
          <Text>
            {task.title} · {task.enabled ? "Enabled" : "Disabled"}
          </Text>
          <Button
            label="Edit automation definition"
            tone="text"
            onPress={() =>
              setDraft(
                JSON.stringify(
                  { type: "automation.upsert", input: { ...task, requireExisting: true } },
                  null,
                  2,
                ),
              )
            }
          />
          <Button
            label={`Queue ${task.enabled ? "disable" : "enable"}`}
            disabled={!canConfigure}
            loading={busy}
            onPress={() =>
              void queue({
                type: "automation.setEnabled",
                input: { id: task.id, enabled: !task.enabled },
              })
            }
          />
        </View>
      ))}
      <HubField label="Automation ID" value={automationId} onChangeText={setAutomationId} />
      <View className="flex-row flex-wrap gap-3">
        <Button
          label="Queue enable"
          disabled={!canConfigure || !automationId.trim()}
          loading={busy}
          onPress={() =>
            void queue({
              type: "automation.setEnabled",
              input: { id: ScheduledTaskId.make(automationId.trim()), enabled: true },
            })
          }
        />
        <Button
          label="Queue disable"
          disabled={!canConfigure || !automationId.trim()}
          loading={busy}
          onPress={() =>
            void queue({
              type: "automation.setEnabled",
              input: { id: ScheduledTaskId.make(automationId.trim()), enabled: false },
            })
          }
        />
      </View>
      <Text className="font-t3-medium">Advanced configuration</Text>
      <Text className="text-sm text-foreground-muted">
        Supply a settings patch, provider change, project change or automation definition. Existing
        secret values are never shown.
      </Text>
      <HubField label="Configuration JSON" value={draft} onChangeText={setDraft} multiline />
      <Button
        label="Queue configuration"
        disabled={!canConfigure}
        loading={busy}
        onPress={() => {
          try {
            const intent = decodeConfigurationIntent(JSON.parse(draft));
            void queue(intent).then((saved) => {
              if (saved) setDraft('{\n  "type": "settings.patch",\n  "patch": {}\n}');
            });
          } catch (error) {
            setMessage(error instanceof Error ? error.message : "Invalid configuration.");
          }
        }}
      />
      {message ? (
        <Text selectable className="text-sm text-foreground-muted">
          {message}
        </Text>
      ) : null}
    </View>
  );
}

function ArchivedSession({
  hubId,
  machineId,
  threadId,
}: {
  readonly hubId: EnvironmentId;
  readonly machineId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const archive = useEnvironmentQuery(
    hubEnvironment.archiveThread({
      environmentId: hubId,
      input: { environmentId: machineId, threadId },
    }),
  );
  const snapshot = archive.data?.kind === "snapshot" ? archive.data : null;
  return (
    <View className="gap-3">
      {archive.error ? <Text className="text-danger">{archive.error}</Text> : null}
      <Button
        label="Refresh session"
        tone="text"
        disabled={archive.isPending}
        onPress={archive.refresh}
      />
      {snapshot ? (
        <Text className="font-t3-medium">{snapshot.projection.thread.title} · Saved session</Text>
      ) : null}
      {snapshot?.projection.messages.map((message) => (
        <View key={message.id} className="gap-1 rounded-xl bg-subtle p-3">
          <Text className="text-xs text-foreground-muted">{message.role}</Text>
          <Text selectable>{message.text}</Text>
          {message.attachments.map((attachment) => (
            <ArchiveAttachment
              key={attachment.id}
              hubId={hubId}
              machineId={machineId}
              attachment={attachment}
            />
          ))}
        </View>
      ))}
      {snapshot?.projection.visibleTurnItems
        .filter(({ item }) => item.type !== "user_message" && item.type !== "assistant_message")
        .map(({ item, sourceItemId, sourceThreadId }) => (
          <ArchiveItem
            key={sourceItemId}
            hubId={hubId}
            machineId={machineId}
            threadId={sourceThreadId}
            turnItemId={sourceItemId}
            title={item.type.replaceAll("_", " ")}
          />
        ))}
      {snapshot?.hasMoreHistory && snapshot.historyCursor ? (
        <ArchiveHistory
          hubId={hubId}
          machineId={machineId}
          threadId={threadId}
          cursor={snapshot.historyCursor}
        />
      ) : null}
    </View>
  );
}

function ArchiveAttachment({
  hubId,
  machineId,
  attachment,
}: {
  readonly hubId: EnvironmentId;
  readonly machineId: EnvironmentId;
  readonly attachment: ChatAttachment;
}) {
  const download = useAtomCommand(hubEnvironment.archiveAttachment);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function share() {
    setBusy(true);
    setError(null);
    try {
      const result = await download({
        environmentId: hubId,
        input: { environmentId: machineId, attachmentId: attachment.id },
      });
      if (result._tag !== "Success") {
        setError(String(squashAtomCommandFailure(result)));
      } else {
        const shared = await shareGeneratedAttachment({
          bytes: result.value,
          attachment,
          signal: new AbortController().signal,
        });
        if (!shared) setError("Sharing is unavailable on this device.");
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not save attachment.");
    }
    setBusy(false);
  }
  return (
    <View className="gap-1">
      <Button
        label={`Save ${attachment.name}`}
        tone="text"
        loading={busy}
        onPress={() => void share()}
      />
      {error ? <Text className="text-danger">{error}</Text> : null}
    </View>
  );
}

function ArchiveHistory({
  hubId,
  machineId,
  threadId,
  cursor,
}: {
  readonly hubId: EnvironmentId;
  readonly machineId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly cursor: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const page = useEnvironmentQuery(
    expanded
      ? hubEnvironment.archiveHistory({
          environmentId: hubId,
          input: { environmentId: machineId, threadId, cursor },
        })
      : null,
  );
  return (
    <View className="gap-3">
      {!expanded ? (
        <Button label="Load older session history" onPress={() => setExpanded(true)} />
      ) : null}
      {page.error ? <Text className="text-danger">{page.error}</Text> : null}
      {page.data?.items.map(({ sourceItemId, sourceThreadId, item }) => (
        <ArchiveItem
          key={sourceItemId}
          hubId={hubId}
          machineId={machineId}
          threadId={sourceThreadId}
          turnItemId={sourceItemId}
          title={item.type.replaceAll("_", " ")}
        />
      ))}
      {page.data?.hasMoreHistory && page.data.nextCursor ? (
        <ArchiveHistory
          key={page.data.nextCursor}
          hubId={hubId}
          machineId={machineId}
          threadId={threadId}
          cursor={page.data.nextCursor}
        />
      ) : null}
    </View>
  );
}

function ArchiveItem({
  hubId,
  machineId,
  threadId,
  turnItemId,
  title,
}: {
  readonly hubId: EnvironmentId;
  readonly machineId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly turnItemId: TurnItemId;
  readonly title: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const detail = useEnvironmentQuery(
    expanded
      ? hubEnvironment.archiveTurnItem({
          environmentId: hubId,
          input: { environmentId: machineId, threadId, turnItemId },
        })
      : null,
  );
  return (
    <View className="gap-2">
      <Button label={title} tone="text" onPress={() => setExpanded(!expanded)} />
      {expanded && detail.error ? <Text className="text-danger">{detail.error}</Text> : null}
      {expanded && detail.data ? (
        <Text selectable className="rounded-xl bg-subtle p-3 font-mono text-sm">
          {archivedItemText(detail.data.item)}
        </Text>
      ) : null}
    </View>
  );
}

function archivedItemText(item: unknown): string {
  if (typeof item !== "object" || item === null) return "No saved detail.";
  const parts = [
    "text",
    "markdown",
    "input",
    "output",
    "fileName",
    "diffStr",
    "oldStr",
    "newStr",
  ].flatMap((key) =>
    key in item && typeof Reflect.get(item, key) === "string"
      ? [String(Reflect.get(item, key))]
      : [],
  );
  return parts.length ? parts.join("\n\n") : JSON.stringify(item, null, 2);
}

function HubField({
  label,
  value,
  onChangeText,
  multiline,
}: {
  readonly label: string;
  readonly value: string;
  readonly onChangeText: (value: string) => void;
  readonly multiline?: boolean;
}) {
  return (
    <View className="gap-1">
      <Text className="text-sm text-foreground-muted">{label}</Text>
      <AppTextInput
        accessibilityLabel={label}
        value={value}
        onChangeText={onChangeText}
        multiline={multiline}
        autoCapitalize="none"
        autoCorrect={false}
        className="min-h-12 rounded-xl bg-subtle px-3 py-2 text-foreground"
      />
    </View>
  );
}
