import { randomUUID } from "~/lib/utils";
import {
  DEFAULT_SERVER_SETTINGS,
  HubConfigurationIntent,
  ServerSettingsPatch,
  type HubConfigurationState,
  type HubObservedConfiguration,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { useState, type FormEvent } from "react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Textarea } from "../ui/textarea";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";

const settingNames = Object.keys(DEFAULT_SERVER_SETTINGS)
  .filter((name) => Object.hasOwn(ServerSettingsPatch.fields, name))
  .sort();
const decodeIntent = Schema.decodeUnknownSync(HubConfigurationIntent);
const label = (value: string) =>
  value.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/^./, (letter) => letter.toUpperCase());

export function HubConfigurationEditor({
  state,
  onSubmit,
  observed,
  onCancel,
  onRetry,
}: {
  readonly state: HubConfigurationState;
  readonly onSubmit: (intent: HubConfigurationIntent) => Promise<boolean>;
  readonly observed: HubObservedConfiguration | null;
  readonly onCancel: (requestId: string) => Promise<void>;
  readonly onRetry: () => Promise<void>;
}) {
  const [section, setSection] = useState("settings");
  const [setting, setSetting] = useState(settingNames[0] ?? "");
  const [value, setValue] = useState("");
  const [title, setTitle] = useState("");
  const [path, setPath] = useState("");
  const [projectId, setProjectId] = useState("");
  const [automationId, setAutomationId] = useState("");
  const [advanced, setAdvanced] = useState('{"type":"settings.patch","patch":{}}');
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const defaultValue: unknown = Object.entries(DEFAULT_SERVER_SETTINGS).find(
    ([key]) => key === setting,
  )?.[1];
  const reportedValue: unknown = observed
    ? Object.entries(observed.settings).find(([key]) => key === setting)?.[1]
    : undefined;
  async function submit(event: FormEvent) {
    event.preventDefault();
    let intent: HubConfigurationIntent;
    try {
      const raw: unknown =
        section === "settings"
          ? {
              type: "settings.patch",
              patch: { [setting]: typeof defaultValue === "string" ? value : JSON.parse(value) },
            }
          : section === "project-edit"
            ? {
                type: "project.update",
                commandId: randomUUID(),
                projectId,
                title,
                workspaceRoot: path,
              }
            : section === "project"
              ? {
                  type: "project.create",
                  commandId: randomUUID(),
                  projectId: randomUUID(),
                  title,
                  workspaceRoot: path,
                }
              : section === "automation"
                ? {
                    type: "automation.setEnabled",
                    input: { id: automationId, enabled: value === "true" },
                  }
                : JSON.parse(advanced);
      intent = decodeIntent(raw);
    } catch {
      setError(
        "Check the values. The change must match this machine’s settings, project, provider or automation format.",
      );
      return;
    }
    setSaving(true);
    setError(null);
    const saved = await onSubmit(intent);
    setSaving(false);
    if (!saved) setError("Could not save this change. Refresh the machine and try again.");
  }
  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Changes are stored on your home server and applied in order when this machine connects.
      </p>
      <form className="space-y-3" onSubmit={(event) => void submit(event)}>
        <Select
          value={section}
          onValueChange={(next) => {
            if (next) {
              setSection(next);
              setValue("");
            }
          }}
        >
          <SelectTrigger aria-label="Configuration section">
            <SelectValue />
          </SelectTrigger>
          <SelectPopup>
            <SelectItem value="settings">Machine settings</SelectItem>
            <SelectItem value="project">Add project</SelectItem>
            {observed ? <SelectItem value="project-edit">Edit project</SelectItem> : null}
            <SelectItem value="automation">Enable or pause automation</SelectItem>
            <SelectItem value="advanced">Advanced configuration</SelectItem>
          </SelectPopup>
        </Select>
        {section === "settings" ? (
          <>
            <Select
              value={setting}
              onValueChange={(next) => {
                if (next) {
                  setSetting(next);
                  setValue("");
                }
              }}
            >
              <SelectTrigger aria-label="Machine setting">
                <SelectValue />
              </SelectTrigger>
              <SelectPopup>
                {settingNames.map((name) => (
                  <SelectItem key={name} value={name}>
                    {label(name)}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
            <label className="grid gap-1.5 text-sm">
              New value
              {typeof defaultValue === "boolean" ? (
                <Select
                  value={value}
                  onValueChange={(next) => {
                    if (next) setValue(next);
                  }}
                >
                  <SelectTrigger aria-label="New setting value">
                    <SelectValue placeholder="Choose value" />
                  </SelectTrigger>
                  <SelectPopup>
                    <SelectItem value="true">On</SelectItem>
                    <SelectItem value="false">Off</SelectItem>
                  </SelectPopup>
                </Select>
              ) : (
                <Input
                  required
                  type={typeof defaultValue === "number" ? "number" : "text"}
                  value={value}
                  onChange={(event) => setValue(event.target.value)}
                  placeholder={
                    typeof defaultValue === "object"
                      ? JSON.stringify(defaultValue)
                      : String(defaultValue ?? "")
                  }
                />
              )}
            </label>
            <p className="text-xs text-muted-foreground">
              Only the selected setting changes.
              {typeof defaultValue === "object"
                ? " Use a JSON value for this structured setting."
                : ""}
            </p>
            {observed ? (
              <p className="break-all text-xs text-muted-foreground">
                Last reported value: {JSON.stringify(reportedValue)}
              </p>
            ) : null}
          </>
        ) : section === "project" || section === "project-edit" ? (
          <>
            {section === "project-edit" && observed ? (
              <Select
                value={projectId}
                onValueChange={(next) => {
                  const project = observed.projects.projects.find((entry) => entry.id === next);
                  if (project) {
                    setProjectId(project.id);
                    setTitle(project.title);
                    setPath(project.workspaceRoot);
                  }
                }}
              >
                <SelectTrigger aria-label="Project">
                  <SelectValue placeholder="Choose project" />
                </SelectTrigger>
                <SelectPopup>
                  {observed.projects.projects
                    .filter((project) => project.deletedAt === null)
                    .map((project) => (
                      <SelectItem key={project.id} value={project.id}>
                        {project.title}
                      </SelectItem>
                    ))}
                </SelectPopup>
              </Select>
            ) : null}
            <label className="grid gap-1.5 text-sm">
              Project name
              <Input required value={title} onChange={(event) => setTitle(event.target.value)} />
            </label>
            <label className="grid gap-1.5 text-sm">
              Folder on the machine
              <Input
                required
                value={path}
                onChange={(event) => setPath(event.target.value)}
                placeholder="/home/you/projects/my-app"
              />
            </label>
          </>
        ) : section === "automation" ? (
          <>
            {observed ? (
              <Select
                value={automationId}
                onValueChange={(next) => {
                  if (next) setAutomationId(next);
                }}
              >
                <SelectTrigger aria-label="Automation">
                  <SelectValue placeholder="Choose automation" />
                </SelectTrigger>
                <SelectPopup>
                  {observed.automation.tasks.map((task) => (
                    <SelectItem key={task.id} value={task.id}>
                      {task.title}
                    </SelectItem>
                  ))}
                </SelectPopup>
              </Select>
            ) : (
              <label className="grid gap-1.5 text-sm">
                Automation ID
                <Input
                  required
                  value={automationId}
                  onChange={(event) => setAutomationId(event.target.value)}
                />
              </label>
            )}
            <Select
              value={value}
              onValueChange={(next) => {
                if (next) setValue(next);
              }}
            >
              <SelectTrigger aria-label="Automation status">
                <SelectValue placeholder="Choose status" />
              </SelectTrigger>
              <SelectPopup>
                <SelectItem value="true">Enabled</SelectItem>
                <SelectItem value="false">Paused</SelectItem>
              </SelectPopup>
            </Select>
          </>
        ) : (
          <>
            <p className="text-xs text-muted-foreground">
              Edit any machine setting, provider, project or automation using a configuration
              operation. Supported operations: settings.patch, provider.mutate, project.create,
              project.update, project.delete, automation.upsert, automation.delete and
              automation.setEnabled.
            </p>
            <label className="grid gap-1.5 text-sm">
              Configuration operation
              <Textarea
                value={advanced}
                onChange={(event) => setAdvanced(event.target.value)}
                spellCheck={false}
              />
            </label>
          </>
        )}
        {error ? (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        ) : null}
        <Button
          type="submit"
          disabled={
            saving ||
            ((section === "automation" || section === "settings") && value === "") ||
            (section === "project-edit" && projectId === "")
          }
        >
          {saving ? "Saving…" : "Save desired configuration"}
        </Button>
      </form>
      {state.entries.length > 0 ? (
        <div className="space-y-2" aria-label="Configuration changes">
          <h4 className="text-sm font-medium">Configuration changes</h4>
          {state.entries.toReversed().map((entry) => (
            <div
              key={entry.requestId}
              className="flex flex-wrap items-center justify-between gap-2 border-t py-2 text-sm"
            >
              <span>
                {label(entry.type)} · revision {entry.revision}
              </span>
              <span
                className={entry.status === "failed" ? "text-destructive" : "text-muted-foreground"}
              >
                {label(entry.status)}
              </span>
              {entry.error ? (
                <p className="w-full text-xs text-destructive">{entry.error}</p>
              ) : null}
              {entry.status === "pending" || entry.status === "failed" ? (
                <Button size="sm" variant="ghost" onClick={() => void onCancel(entry.requestId)}>
                  Cancel change
                </Button>
              ) : null}
            </div>
          ))}
        </div>
      ) : null}
      {state.entries.some((entry) => entry.status === "failed") ? (
        <Button variant="outline" size="sm" onClick={() => void onRetry()}>
          Retry failed changes
        </Button>
      ) : null}
    </div>
  );
}
