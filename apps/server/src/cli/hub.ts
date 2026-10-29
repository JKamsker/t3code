import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as HubWorkerConfig from "../hub/HubWorkerConfig.ts";
import { authLocationFlags, type CliAuthLocationFlags, resolveCliAuthConfig } from "./config.ts";
import { runServerCommand } from "./server.ts";
import { sharedServerCommandFlags } from "./config.ts";

const runWithHub = Effect.fn("hub.runWithHub")(function* <A, E>(
  flags: CliAuthLocationFlags,
  run: (hub: HubWorkerConfig.HubWorkerConfig["Service"]) => Effect.Effect<A, E>,
) {
  const logLevel = yield* GlobalFlag.LogLevel;
  const config = yield* resolveCliAuthConfig(flags, logLevel);
  return yield* HubWorkerConfig.HubWorkerConfig.pipe(
    Effect.flatMap(run),
    Effect.provide(
      HubWorkerConfig.layer.pipe(
        Layer.provide(ServerEnvironment.layer.pipe(Layer.provide(ServerSecretStore.layer))),
        Layer.provide(Layer.mergeAll(ServerSecretStore.layer, FetchHttpClient.layer)),
        Layer.provide(ServerConfig.layer(config)),
        Layer.provide(Layer.succeed(References.MinimumLogLevel, "Error")),
      ),
    ),
  );
});
const hubServeCommand = Command.make("serve", { ...sharedServerCommandFlags }).pipe(
  Command.withDescription(
    "Run a home hub. Pair clients with `t3 pair` and enroll machines from Settings → Connections → Home server.",
  ),
  Command.withHandler((flags) =>
    runServerCommand(flags, {
      startupPresentation: "headless",
      forceAutoBootstrapProjectFromCwd: false,
    }),
  ),
);
const hubJoinCommand = Command.make("join", {
  ...authLocationFlags,
  url: Argument.String("url").pipe(Argument.withDescription("The home hub URL.")),
  code: Flag.String("code").pipe(
    Flag.withDescription("A one-time machine enrollment code from the hub."),
  ),
}).pipe(
  Command.withDescription(
    "Enroll this machine with a self-hosted hub. Start or restart its server to connect.",
  ),
  Command.withHandler((flags) =>
    runWithHub(flags, (hub) =>
      hub
        .join({ hubUrl: flags.url, code: flags.code })
        .pipe(
          Effect.andThen(
            Console.log(
              "Machine enrolled. Start or restart its T3 Code server to connect to the hub.",
            ),
          ),
        ),
    ),
  ),
);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
export const renderHubStatus = Effect.fn("hub.renderStatus")(function* (
  saved: HubWorkerConfig.HubWorkerLink | null,
  json: boolean,
) {
  const link =
    saved === null ? null : { hubUrl: saved.hubUrl, hubId: saved.hubId, workerId: saved.workerId };
  return json
    ? yield* encodeJson({ hub: link })
    : link === null
      ? "This machine has no home hub."
      : `Hub: ${link.hubUrl}\nHub ID: ${link.hubId}\nMachine ID: ${link.workerId}`;
});
const hubStatusCommand = Command.make("status", {
  ...authLocationFlags,
  json: Flag.Boolean("json").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Emit a JSON object with the saved hub link."),
  ),
}).pipe(
  Command.withDescription("Show this machine's saved hub enrollment, without credentials."),
  Command.withHandler((flags) =>
    runWithHub(flags, (hub) =>
      Effect.gen(function* () {
        yield* Console.log(yield* renderHubStatus(yield* hub.status, flags.json));
      }),
    ),
  ),
);
const hubLeaveCommand = Command.make("leave", { ...authLocationFlags }).pipe(
  Command.withDescription(
    "Remove this machine's saved hub enrollment. Restart its server to disconnect.",
  ),
  Command.withHandler((flags) =>
    runWithHub(flags, (hub) =>
      hub.leave.pipe(
        Effect.andThen(Console.log("Hub enrollment removed. Restart the server to disconnect.")),
      ),
    ),
  ),
);
export const hubCommand = Command.make("hub").pipe(
  Command.withDescription("Host a private home hub or connect this machine to one."),
  Command.withSubcommands([hubServeCommand, hubJoinCommand, hubStatusCommand, hubLeaveCommand]),
);
