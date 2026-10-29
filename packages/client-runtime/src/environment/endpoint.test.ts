import { describe, expect, it } from "vite-plus/test";

import { deriveWsBaseUrl, environmentEndpointUrl, normalizeHttpBaseUrl } from "./endpoint.ts";

describe("advertised endpoint helpers", () => {
  it("normalizes HTTP and WebSocket base URLs", () => {
    expect(normalizeHttpBaseUrl("https://example.com/path?x=1#hash")).toBe("https://example.com/");
    expect(normalizeHttpBaseUrl("wss://example.com/socket")).toBe("https://example.com/");
    expect(deriveWsBaseUrl("https://example.com/api")).toBe("wss://example.com/");
    expect(deriveWsBaseUrl("http://127.0.0.1:3773")).toBe("ws://127.0.0.1:3773/");
  });

  it("keeps an environment route prefix for metadata, authentication, and media", () => {
    const base = "https://hub.example/api/hub/environments/worker/";
    expect(environmentEndpointUrl(base, "/.well-known/t3/environment")).toBe(
      `${base}.well-known/t3/environment`,
    );
    expect(environmentEndpointUrl(base, "/api/auth/ws-ticket")).toBe(`${base}api/auth/ws-ticket`);
    expect(environmentEndpointUrl(base, "/api/assets/file?size=1")).toBe(
      `${base}api/assets/file?size=1`,
    );
    expect(environmentEndpointUrl("https://worker.example/", "/api/auth/session")).toBe(
      "https://worker.example/api/auth/session",
    );
  });
});
