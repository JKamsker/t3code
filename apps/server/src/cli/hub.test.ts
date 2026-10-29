import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { renderHubStatus } from "./hub.ts";

const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

it.effect(
  "shows hub status for people and JSON consumers without exposing the worker credential",
  () =>
    Effect.gen(function* () {
      const saved = {
        hubUrl: "https://hub.example",
        hubId: EnvironmentId.make("hub-1"),
        workerId: "worker-1",
        workerToken: "private-worker-credential",
      };
      const human = yield* renderHubStatus(saved, false);
      const machine = yield* renderHubStatus(saved, true);
      expect(human).toBe("Hub: https://hub.example\nHub ID: hub-1\nMachine ID: worker-1");
      expect(human).not.toContain(saved.workerToken);
      expect(machine).not.toContain(saved.workerToken);
      expect(yield* decodeJson(machine)).toEqual({
        hub: { hubUrl: saved.hubUrl, hubId: saved.hubId, workerId: saved.workerId },
      });
      expect(yield* renderHubStatus(null, false)).toBe("This machine has no home hub.");
      expect(yield* renderHubStatus(null, true)).toBe('{"hub":null}');
    }),
);
