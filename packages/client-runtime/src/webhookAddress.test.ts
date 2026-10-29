import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";

import { HubConnectionTarget } from "./connection/model.ts";
import { webhookAddress } from "./webhookAddress.ts";

describe("webhook addresses", () => {
  it.each([
    {
      url: null,
      target: undefined,
      address: "https://machine.test/api/hooks/token",
      copyable: true,
    },
    {
      url: null,
      target: new HubConnectionTarget({
        environmentId: EnvironmentId.make("worker"),
        hubEnvironmentId: EnvironmentId.make("hub"),
        label: "Worker",
      }),
      address: "/api/hooks/token",
      copyable: false,
    },
    {
      url: "https://worker-public.test/api/hooks/token",
      target: new HubConnectionTarget({
        environmentId: EnvironmentId.make("worker"),
        hubEnvironmentId: EnvironmentId.make("hub"),
        label: "Worker",
      }),
      address: "https://worker-public.test/api/hooks/token",
      copyable: true,
    },
  ])("retains direct public URLs and prevents local paths naming the hub", (row) => {
    expect(
      webhookAddress(
        { path: "/api/hooks/token", url: row.url, hasSecret: false },
        "https://machine.test/api/hub/environments/worker/",
        row.target,
      ),
    ).toMatchObject({ address: row.address, copyable: row.copyable });
  });
});
