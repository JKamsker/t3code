import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";

export class HubSynchronization extends Context.Service<
  HubSynchronization,
  {
    readonly ready: Effect.Effect<boolean>;
    readonly pause: Effect.Effect<void>;
    readonly resume: Effect.Effect<void>;
  }
>()("t3/hub/HubSynchronization") {}

const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const link = yield* secrets.get("hub-worker-link");
  const ready = yield* Ref.make(Option.isNone(link));
  return HubSynchronization.of({
    ready: Ref.get(ready),
    pause: Ref.set(ready, false),
    resume: Ref.set(ready, true),
  });
});

export const layer = Layer.effect(HubSynchronization, make);
