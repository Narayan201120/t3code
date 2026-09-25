import type { OpencodeClient } from "@opencode-ai/sdk/v2";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it as effectIt } from "@effect/vitest";
import {
  HostProcessEnvironment,
  HostProcessExecutablePath,
  HostProcessPlatform,
} from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { describe, expect, it } from "vite-plus/test";

import {
  OpenCodeRuntime,
  OpenCodeRuntimeError,
  OpenCodeRuntimeLive,
  resolveOpenCodeConfigContent,
  resolveOpenCodeServerPassword,
  verifyOpenCodeServerVersion,
} from "./opencodeRuntime.ts";

describe("resolveOpenCodeConfigContent", () => {
  it("prefers the caller environment over the inherited environment", () => {
    expect(
      resolveOpenCodeConfigContent(
        { OPENCODE_CONFIG_CONTENT: '{"source":"caller"}' },
        { OPENCODE_CONFIG_CONTENT: '{"source":"process"}' },
      ),
    ).toBe('{"source":"caller"}');
  });

  it("falls back to the inherited environment and then an empty config", () => {
    expect(
      resolveOpenCodeConfigContent(undefined, {
        OPENCODE_CONFIG_CONTENT: '{"source":"process"}',
      }),
    ).toBe('{"source":"process"}');
    expect(resolveOpenCodeConfigContent(undefined, {})).toBe("{}");
  });
});

describe("resolveOpenCodeServerPassword", () => {
  it("uses the local environment password when settings do not provide one", () => {
    expect(
      resolveOpenCodeServerPassword(
        { external: false, environment: { OPENCODE_SERVER_PASSWORD: " env password " } },
        {},
      ),
    ).toBe(" env password ");
  });

  it("uses the settings password for a local server", () => {
    expect(
      resolveOpenCodeServerPassword({ external: false, serverPassword: " settings password " }, {}),
    ).toBe(" settings password ");
  });

  it("uses the settings password when local settings and environment differ", () => {
    expect(
      resolveOpenCodeServerPassword(
        {
          external: false,
          serverPassword: "settings-password",
          environment: { OPENCODE_SERVER_PASSWORD: "environment-password" },
        },
        {},
      ),
    ).toBe("settings-password");
  });

  it("does not send an inherited local password to an external server", () => {
    expect(
      resolveOpenCodeServerPassword(
        { external: true, environment: { OPENCODE_SERVER_PASSWORD: "local-secret" } },
        { OPENCODE_SERVER_PASSWORD: "inherited-secret" },
      ),
    ).toBeUndefined();
  });
});

function makeHealthClient(
  result: (options?: { readonly signal?: AbortSignal }) => Promise<unknown>,
): OpencodeClient {
  return {
    global: {
      health: result,
    },
  } as unknown as OpencodeClient;
}

describe("verifyOpenCodeServerVersion", () => {
  effectIt.effect("accepts a supported server version", () =>
    Effect.gen(function* () {
      const version = yield* verifyOpenCodeServerVersion(
        makeHealthClient(() => Promise.resolve({ data: { healthy: true, version: "1.14.19" } })),
      );
      expect(version).toBe("1.14.19");
    }),
  );

  effectIt.effect("rejects a server below the supported version", () =>
    Effect.gen(function* () {
      const error = yield* verifyOpenCodeServerVersion(
        makeHealthClient(() => Promise.resolve({ data: { healthy: true, version: "1.14.18" } })),
      ).pipe(Effect.flip);
      expect(error).toBeInstanceOf(OpenCodeRuntimeError);
      expect(error.detail).toContain("v1.14.18 is too old");
    }),
  );

  for (const data of [
    { healthy: true },
    { healthy: true, version: "not-a-version" },
    { healthy: false, version: "1.14.19" },
  ]) {
    effectIt.effect(`rejects an invalid health response: ${JSON.stringify(data)}`, () =>
      Effect.gen(function* () {
        const error = yield* verifyOpenCodeServerVersion(
          makeHealthClient(() => Promise.resolve({ data })),
        ).pipe(Effect.flip);
        expect(error).toBeInstanceOf(OpenCodeRuntimeError);
        expect(error.detail).toContain("requires OpenCode v1.14.19 or newer");
      }),
    );
  }

  effectIt.effect("preserves an unauthorized health error", () =>
    Effect.gen(function* () {
      const error = yield* verifyOpenCodeServerVersion(
        makeHealthClient(() =>
          Promise.reject({ response: { status: 401 }, error: { message: "Unauthorized" } }),
        ),
      ).pipe(Effect.flip);
      expect(error).toBeInstanceOf(OpenCodeRuntimeError);
      expect(error.detail).toContain("status=401");
      expect(error.detail).toContain("Unauthorized");
    }),
  );

  effectIt.effect("aborts a health request when the version check times out", () =>
    Effect.gen(function* () {
      let requestSignal: AbortSignal | undefined;
      const checkFiber = yield* verifyOpenCodeServerVersion(
        makeHealthClient((options) => {
          requestSignal = options?.signal;
          return new Promise(() => undefined);
        }),
      ).pipe(Effect.flip, Effect.forkChild);

      yield* Effect.yieldNow;
      expect(requestSignal).toBeDefined();
      yield* TestClock.adjust("6 seconds");

      const error = yield* Fiber.join(checkFiber);
      expect(error.detail).toBe("Timed out while checking the OpenCode server version.");
      expect(requestSignal?.aborted).toBe(true);
    }).pipe(Effect.provide(TestClock.layer())),
  );
});

describe("OpenCode server output", () => {
  effectIt.live(
    "drains stdout and stderr after startup so server requests can finish",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const environment = yield* HostProcessEnvironment;
        const executablePath = yield* HostProcessExecutablePath;
        const platform = yield* HostProcessPlatform;
        const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode-output-" });
        const isWindows = platform === "win32";
        const binaryPath = path.join(tempDir, isWindows ? "opencode.cmd" : "opencode");
        const scriptPath = path.join(tempDir, "opencode.mjs");

        yield* fs.writeFileString(
          scriptPath,
          `import { createServer } from "node:http";
const writeOutput = (stream) => new Promise((resolve, reject) => {
  stream.write("x".repeat(2 * 1024 * 1024), (error) => error ? reject(error) : resolve());
});
const server = createServer(async (request, response) => {
  if (request.url.startsWith("/global/health")) {
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({ healthy: true, version: "1.14.19" }));
    return;
  }
  await Promise.all([writeOutput(process.stdout), writeOutput(process.stderr)]);
  response.end("drained");
});
server.listen(0, "127.0.0.1", () => {
  process.stdout.write("opencode server listening on http://127.0.0.1:" + server.address().port + "\\n");
});
`,
        );
        yield* fs.writeFileString(
          binaryPath,
          [
            ...(isWindows ? ["@echo off"] : ["#!/bin/sh"]),
            isWindows
              ? '"%T3_TEST_NODE_BINARY%" "%T3_TEST_OPENCODE_SCRIPT%" %*'
              : 'exec "$T3_TEST_NODE_BINARY" "$T3_TEST_OPENCODE_SCRIPT" "$@"',
            "",
          ].join("\n"),
        );
        if (!isWindows) {
          yield* fs.chmod(binaryPath, 0o755);
        }

        const runtime = yield* OpenCodeRuntime;
        const server = yield* runtime.startOpenCodeServerProcess({
          binaryPath,
          directory: tempDir,
          port: 0,
          environment: {
            ...environment,
            T3_TEST_NODE_BINARY: executablePath,
            T3_TEST_OPENCODE_SCRIPT: scriptPath,
          },
        });
        const response = yield* HttpClient.get(`${server.url}/output`);

        expect(yield* response.text).toBe("drained");
        expect(yield* server.isRunning).toBe(true);
      }).pipe(
        Effect.scoped,
        Effect.provide([
          OpenCodeRuntimeLive.pipe(Layer.provideMerge(NodeServices.layer)),
          FetchHttpClient.layer,
        ]),
      ),
    10_000,
  );

  effectIt.live(
    "starts a v2-style server from the new banner and uses its printed password",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const environment = yield* HostProcessEnvironment;
        const executablePath = yield* HostProcessExecutablePath;
        const platform = yield* HostProcessPlatform;
        const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode-v2-" });
        const isWindows = platform === "win32";
        const binaryPath = path.join(tempDir, isWindows ? "opencode.cmd" : "opencode");
        const scriptPath = path.join(tempDir, "opencode.mjs");

        yield* fs.writeFileString(
          scriptPath,
          `import { createServer } from "node:http";
const json = (response, value) => {
  response.setHeader("Content-Type", "application/json");
  response.end(JSON.stringify(value));
};
const expectedAuth = "Basic " + Buffer.from("opencode:test-password-123", "utf8").toString("base64");
let modelHits = 0;
const server = createServer((request, response) => {
  if (request.url.startsWith("/global/health")) {
    response.statusCode = 200;
    response.setHeader("Content-Type", "text/html");
    response.end("<html>opencode</html>");
    return;
  }
  if (request.headers.authorization !== expectedAuth) {
    response.statusCode = 401;
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({ message: "Unauthorized", got: request.headers.authorization ?? null }));
    return;
  }
  if (request.url.startsWith("/api/info")) {
    json(response, { version: "2.0.16", pid: 1, urls: [], paths: { tmp: "/tmp" } });
    return;
  }
  if (request.url.startsWith("/api/provider")) {
    json(response, { location: {}, data: [{ id: "openai", name: "OpenAI" }] });
    return;
  }
  if (request.url.startsWith("/api/model")) {
    modelHits += 1;
    if (modelHits <= 2) {
      json(response, { location: {}, data: [] });
      return;
    }
    json(response, { location: {}, data: [
      { providerID: "openai", modelID: "gpt-5", name: "GPT-5", variants: [{ id: "high" }] },
      { providerID: "other", modelID: "other-1", name: "Other 1", variants: [] },
    ] });
    return;
  }
  if (request.url.startsWith("/api/agent")) {
    json(response, { location: {}, data: [{ name: "build", mode: "primary", hidden: false }] });
    return;
  }
  if (request.url.startsWith("/api/skill")) {
    json(response, { location: {}, data: [{ name: "review", description: "Reviews", path: "/skills/review/SKILL.md" }] });
    return;
  }
  if (request.url.startsWith("/api/command")) {
    json(response, { location: {}, data: [{ name: "init", description: "Init" }] });
    return;
  }
  response.statusCode = 404;
  response.end("nope");
});
server.listen(0, "127.0.0.1", () => {
  process.stdout.write("server listening on http://127.0.0.1:" + server.address().port + "\\n");
  process.stdout.write("server password test-password-123\\n");
});
`,
        );
        yield* fs.writeFileString(
          binaryPath,
          [
            ...(isWindows ? ["@echo off"] : ["#!/bin/sh"]),
            isWindows
              ? '"%T3_TEST_NODE_BINARY%" "%T3_TEST_OPENCODE_SCRIPT%" %*'
              : 'exec "$T3_TEST_NODE_BINARY" "$T3_TEST_OPENCODE_SCRIPT" "$@"',
            "",
          ].join("\n"),
        );
        if (!isWindows) {
          yield* fs.chmod(binaryPath, 0o755);
        }

        const runtime = yield* OpenCodeRuntime;
        const server = yield* runtime.startOpenCodeServerProcess({
          binaryPath,
          directory: tempDir,
          port: 0,
          environment: {
            ...environment,
            T3_TEST_NODE_BINARY: executablePath,
            T3_TEST_OPENCODE_SCRIPT: scriptPath,
          },
        });

        expect(server.apiVersion).toBe("v2");
        expect(server.version).toBe("2.0.16");
        expect(server.serverPassword).toBe("test-password-123");

        const inventory = yield* runtime.loadOpenCodeInventoryV2(
          runtime.createOpenCodeV2Client({
            baseUrl: server.url,
            ...(server.serverPassword !== undefined
              ? { serverPassword: server.serverPassword }
              : {}),
          }),
          tempDir,
        );
        expect(inventory.connectedProviders).toEqual(["openai"]);
        expect(inventory.models.map((model) => `${model.providerID}/${model.modelID}`)).toEqual([
          "openai/gpt-5",
          "other/other-1",
        ]);
        expect(inventory.models[0]?.variantIDs).toEqual(["high"]);
        expect(inventory.agents).toEqual([{ name: "build", mode: "primary", hidden: false }]);
        expect(inventory.skills).toEqual([
          { name: "review", description: "Reviews", location: "/skills/review/SKILL.md" },
        ]);
        expect(inventory.commands).toEqual([{ name: "init", description: "Init" }]);
      }).pipe(
        Effect.scoped,
        Effect.provide([OpenCodeRuntimeLive.pipe(Layer.provideMerge(NodeServices.layer))]),
      ),
    15_000,
  );
});
