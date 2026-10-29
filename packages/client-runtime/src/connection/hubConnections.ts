import type { EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SubscriptionRef from "effect/SubscriptionRef";

import type { PreparedConnection } from "./model.ts";

export class HubConnections extends Context.Service<
  HubConnections,
  SubscriptionRef.SubscriptionRef<ReadonlyMap<EnvironmentId, PreparedConnection>>
>()("@t3tools/client-runtime/connection/hubConnections") {}

export const layer = Layer.effect(
  HubConnections,
  Effect.map(
    SubscriptionRef.make<ReadonlyMap<EnvironmentId, PreparedConnection>>(new Map()),
    HubConnections.of,
  ),
);
