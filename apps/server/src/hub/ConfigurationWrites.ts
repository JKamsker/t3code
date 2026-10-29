import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";

type WithLock = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
const unlocked: WithLock = (effect) => effect;

class ConfigurationWrites extends Context.Reference<WithLock>("t3/hub/ConfigurationWrites", {
  defaultValue: () => unlocked,
}) {}

// Nested project/settings/task writes share the outer configuration transaction.
export const atomic = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.flatMap(ConfigurationWrites, (withLock) =>
    withLock(Effect.provideService(effect, ConfigurationWrites, unlocked)),
  );

export const layer = Layer.effect(
  ConfigurationWrites,
  Effect.map(
    Semaphore.make(1),
    (semaphore): WithLock =>
      (effect) =>
        semaphore.withPermit(effect),
  ),
);
