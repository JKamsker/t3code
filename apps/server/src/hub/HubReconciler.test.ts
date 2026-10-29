import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DEFAULT_SERVER_SETTINGS,
  CommandId,
  IsoDateTime,
  ProjectId,
  ScheduledTaskId,
  type Project,
  type ServerSettings,
  ProviderInstanceId,
  ProviderDriverKind,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as ConfigurationWrites from "./ConfigurationWrites.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerSettingsService from "../serverSettings.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as HubReconciler from "./HubReconciler.ts";

it.effect(
  "persists scoped receipts across worker restarts and rejects request reuse for another intent",
  () =>
    Effect.gen(function* () {
      const values = new Map<string, Uint8Array>();
      let projectsApplied = 0;
      let automationsApplied = 0;
      const now = IsoDateTime.make("2026-01-01T00:00:00.000Z");
      const project: Project = {
        id: ProjectId.make("project-1"),
        title: "Project",
        workspaceRoot: "/project",
        defaultModelSelection: null,
        scripts: [],
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
      };
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ServerSecretStore.ServerSecretStore, {
          remove: (key) =>
            Effect.sync(() => {
              values.delete(key);
            }),
          get: (key) => Effect.sync(() => Option.fromUndefinedOr(values.get(key))),
          set: (key, value) =>
            Effect.sync(() => {
              values.set(key, value);
            }),
        }),
        Layer.mock(ServerSettingsService.ServerSettingsService, {
          getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
        }),
        Layer.mock(ProjectService.ProjectService, {
          snapshot: Effect.succeed({ projects: [], updatedAt: now }),
          create: () =>
            Effect.sync(() => {
              projectsApplied++;
              return project;
            }),
        }),
        Layer.mock(ScheduledTasks.ScheduledTaskService, {
          list: () => Effect.succeed({ tasks: [] }),
          delete: (input) =>
            Effect.sync(() => {
              automationsApplied++;
              return input;
            }),
        }),
      );
      const projectInput = {
        requestId: "hub-1:worker-1:request-1",
        intent: {
          type: "project.create" as const,
          commandId: CommandId.make("command-1"),
          projectId: project.id,
          title: project.title,
          workspaceRoot: project.workspaceRoot,
        },
      };
      const automationInput = {
        requestId: "hub-1:worker-1:request-2",
        intent: {
          type: "automation.delete" as const,
          input: { id: ScheduledTaskId.make("task-1") },
        },
      };
      const first = yield* HubReconciler.layer.pipe(Layer.provide(dependencies), Layer.build);
      yield* Effect.gen(function* () {
        const worker = yield* HubReconciler.HubReconciler;
        yield* worker.apply(projectInput);
        yield* worker.apply(automationInput);
      }).pipe(Effect.provide(first));
      const restarted = yield* HubReconciler.layer.pipe(Layer.provide(dependencies), Layer.build);
      yield* Effect.gen(function* () {
        const worker = yield* HubReconciler.HubReconciler;
        yield* worker.apply(projectInput);
        yield* worker.apply(automationInput);
        expect(projectsApplied).toBe(1);
        expect(automationsApplied).toBe(1);
        expect(
          (yield* worker
            .apply({ ...projectInput, intent: { ...projectInput.intent, title: "Changed" } })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
      }).pipe(Effect.provide(restarted));
    }),
);

it.effect("hashes actual secret values while returning a redacted observed snapshot", () =>
  Effect.gen(function* () {
    let settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        [ProviderInstanceId.make("antigravity")]: {
          driver: ProviderDriverKind.make("antigravity"),
          config: { apiKey: "first-secret" },
        },
      },
    };
    const dependencies = Layer.mergeAll(
      NodeServices.layer,
      Layer.mock(ServerSecretStore.ServerSecretStore, {}),
      Layer.mock(ServerSettingsService.ServerSettingsService, {
        getSettings: Effect.sync(() => settings),
      }),
      Layer.mock(ProjectService.ProjectService, {
        snapshot: Effect.succeed({
          projects: [],
          updatedAt: IsoDateTime.make("2026-01-01T00:00:00.000Z"),
        }),
      }),
      Layer.mock(ScheduledTasks.ScheduledTaskService, {
        list: () => Effect.succeed({ tasks: [] }),
      }),
    );
    const context = yield* HubReconciler.layer.pipe(Layer.provide(dependencies), Layer.build);
    yield* Effect.gen(function* () {
      const worker = yield* HubReconciler.HubReconciler;
      const first = yield* worker.snapshot;
      expect(
        first.settings.providerInstances[ProviderInstanceId.make("antigravity")]?.config,
      ).toEqual({ apiKey: "••••••" });
      settings = {
        ...settings,
        providerInstances: {
          [ProviderInstanceId.make("antigravity")]: {
            driver: ProviderDriverKind.make("antigravity"),
            config: { apiKey: "second-secret" },
          },
        },
      };
      expect((yield* worker.snapshot).hashes.settings).not.toBe(first.hashes.settings);
    }).pipe(Effect.provide(context));
  }),
);

it.effect("chains offline edits from one baseline and preserves an intervening local change", () =>
  Effect.gen(function* () {
    let settings = DEFAULT_SERVER_SETTINGS;
    let mutations = 0;
    const values = new Map<string, Uint8Array>();
    const dependencies = Layer.mergeAll(
      NodeServices.layer,
      Layer.mock(ServerSecretStore.ServerSecretStore, {
        remove: (key) =>
          Effect.sync(() => {
            values.delete(key);
          }),
        get: (key) => Effect.sync(() => Option.fromUndefinedOr(values.get(key))),
        set: (key, value) =>
          Effect.sync(() => {
            values.set(key, value);
          }),
      }),
      Layer.mock(ServerSettingsService.ServerSettingsService, {
        getSettings: Effect.sync(() => settings),
        updateSettings: (patch) =>
          Effect.sync(() => {
            mutations++;
            settings = {
              ...settings,
              addProjectBaseDirectory:
                patch.addProjectBaseDirectory ?? settings.addProjectBaseDirectory,
            };
            return settings;
          }),
      }),
      Layer.mock(ProjectService.ProjectService, {
        snapshot: Effect.succeed({
          projects: [],
          updatedAt: IsoDateTime.make("2026-01-01T00:00:00.000Z"),
        }),
      }),
      Layer.mock(ScheduledTasks.ScheduledTaskService, {
        list: () => Effect.succeed({ tasks: [] }),
      }),
    );
    const first = yield* HubReconciler.layer.pipe(Layer.provide(dependencies), Layer.build);
    const expectedHash = yield* Effect.gen(function* () {
      const worker = yield* HubReconciler.HubReconciler;
      const baseline = (yield* worker.snapshot).hashes.settings!;
      yield* worker.apply(
        {
          requestId: "edit-1",
          expectedHash: baseline,
          intent: { type: "settings.patch", patch: { addProjectBaseDirectory: "light" } },
        },
        "hub-1:worker-1",
      );
      return baseline;
    }).pipe(Effect.provide(first));
    const restarted = yield* HubReconciler.layer.pipe(Layer.provide(dependencies), Layer.build);
    yield* Effect.gen(function* () {
      const worker = yield* HubReconciler.HubReconciler;
      yield* worker.apply(
        {
          requestId: "edit-2",
          expectedHash,
          intent: { type: "settings.patch", patch: { addProjectBaseDirectory: "dark" } },
        },
        "hub-1:worker-1",
      );
      expect(settings.addProjectBaseDirectory).toBe("dark");
      settings = { ...settings, addProjectBaseDirectory: "local-edit" };
      const result = yield* worker
        .apply(
          {
            requestId: "edit-3",
            expectedHash,
            intent: { type: "settings.patch", patch: { addProjectBaseDirectory: "light" } },
          },
          "hub-1:worker-1",
        )
        .pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      expect(settings.addProjectBaseDirectory).toBe("local-edit");
      expect(mutations).toBe(2);
    }).pipe(Effect.provide(restarted));
  }),
);

it.effect("serializes local settings writes with hub baseline validation and mutation", () =>
  Effect.gen(function* () {
    const readStarted = yield* Deferred.make<void>();
    const releaseRead = yield* Deferred.make<void>();
    const localStarted = yield* Deferred.make<void>();
    let pauseReads = false;
    const coordinatedSettings = Layer.effect(
      ServerSettingsService.ServerSettingsService,
      Effect.gen(function* () {
        const actual = yield* ServerSettingsService.ServerSettingsService;
        return {
          ...actual,
          getSettings: actual.getSettings.pipe(
            Effect.tap(() =>
              pauseReads
                ? Deferred.succeed(readStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseRead)),
                  )
                : Effect.void,
            ),
          ),
        };
      }),
    ).pipe(Layer.provide(ServerSettingsService.layerTest()));
    const values = new Map<string, Uint8Array>();
    const dependencies = Layer.mergeAll(
      NodeServices.layer,
      ConfigurationWrites.layer,
      coordinatedSettings,
      Layer.mock(ServerSecretStore.ServerSecretStore, {
        remove: (key) =>
          Effect.sync(() => {
            values.delete(key);
          }),
        get: (key) => Effect.sync(() => Option.fromUndefinedOr(values.get(key))),
        set: (key, value) =>
          Effect.sync(() => {
            values.set(key, value);
          }),
      }),
      Layer.mock(ProjectService.ProjectService, {
        snapshot: Effect.succeed({
          projects: [],
          updatedAt: IsoDateTime.make("2026-01-01T00:00:00.000Z"),
        }),
      }),
      Layer.mock(ScheduledTasks.ScheduledTaskService, {
        list: () => Effect.succeed({ tasks: [] }),
      }),
    );
    yield* Effect.gen(function* () {
      const worker = yield* HubReconciler.HubReconciler;
      const settings = yield* ServerSettingsService.ServerSettingsService;
      const expectedHash = (yield* worker.snapshot).hashes.settings!;
      pauseReads = true;
      const hubWrite = yield* worker
        .apply(
          {
            requestId: "hub-edit",
            expectedHash,
            intent: { type: "settings.patch", patch: { addProjectBaseDirectory: "hub-value" } },
          },
          "hub:worker",
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(readStarted);
      const localWrite = yield* Deferred.succeed(localStarted, undefined).pipe(
        Effect.andThen(settings.updateSettings({ addProjectBaseDirectory: "local-value" })),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(localStarted);
      yield* Effect.sync(() => expect(localWrite.pollUnsafe()).toBeUndefined()).pipe(
        Effect.ensuring(Deferred.succeed(releaseRead, undefined)),
      );
      yield* Fiber.join(hubWrite);
      yield* Fiber.join(localWrite);
      expect((yield* settings.getSettings).addProjectBaseDirectory).toBe("local-value");
    }).pipe(Effect.provide(HubReconciler.layer.pipe(Layer.provideMerge(dependencies))));
  }),
);

it.effect(
  "does not repeat an ambiguous committed mutation after the apply receipt could not be saved",
  () =>
    Effect.gen(function* () {
      let settings = DEFAULT_SERVER_SETTINGS;
      let mutations = 0;
      let failReceipt = true;
      const values = new Map<string, Uint8Array>();
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        ConfigurationWrites.layer,
        Layer.mock(ServerSecretStore.ServerSecretStore, {
          get: (key) => Effect.sync(() => Option.fromUndefinedOr(values.get(key))),
          remove: (key) =>
            Effect.sync(() => {
              values.delete(key);
            }),
          set: (key, value) =>
            Effect.gen(function* () {
              if (
                failReceipt &&
                key.startsWith("hub-configuration-receipt-") &&
                !key.endsWith("-prepared")
              ) {
                failReceipt = false;
                return yield* new ServerSecretStore.SecretStorePersistError({
                  resource: "test apply receipt",
                  cause: undefined,
                });
              }
              values.set(key, value);
            }),
        }),
        Layer.mock(ServerSettingsService.ServerSettingsService, {
          getSettings: Effect.sync(() => settings),
          updateSettings: (patch) =>
            Effect.sync(() => {
              mutations++;
              settings = {
                ...settings,
                addProjectBaseDirectory:
                  patch.addProjectBaseDirectory ?? settings.addProjectBaseDirectory,
              };
              return settings;
            }),
        }),
        Layer.mock(ProjectService.ProjectService, {
          snapshot: Effect.succeed({
            projects: [],
            updatedAt: IsoDateTime.make("2026-01-01T00:00:00.000Z"),
          }),
        }),
        Layer.mock(ScheduledTasks.ScheduledTaskService, {
          list: () => Effect.succeed({ tasks: [] }),
        }),
      );
      const request = {
        requestId: "interrupted-edit",
        intent: {
          type: "settings.patch" as const,
          patch: { addProjectBaseDirectory: "hub-value" },
        },
      };
      const first = yield* HubReconciler.layer.pipe(Layer.provide(dependencies), Layer.build);
      yield* Effect.gen(function* () {
        const worker = yield* HubReconciler.HubReconciler;
        expect((yield* worker.apply(request, "hub:worker").pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect(settings.addProjectBaseDirectory).toBe("hub-value");
      }).pipe(Effect.provide(first));
      const restarted = yield* HubReconciler.layer.pipe(Layer.provide(dependencies), Layer.build);
      yield* Effect.gen(function* () {
        const worker = yield* HubReconciler.HubReconciler;
        const replay = yield* worker.apply(request, "hub:worker").pipe(Effect.result);
        expect(replay._tag).toBe("Failure");
        if (replay._tag === "Failure") expect(replay.failure.reason).toBe("recovery");
        expect(mutations).toBe(1);
      }).pipe(Effect.provide(restarted));
    }),
);
