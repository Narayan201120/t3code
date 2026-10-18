// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import type { OpenCodeSettings } from "../settings.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderMaintenance from "@t3tools/provider-core/server/maintenanceResolver";
import * as OpenCodeRuntime from "./OpenCodeRuntime.ts";
import {
  OPENCODE_2_RESPONSES,
  OPENCODE_2_WORKSPACE_RESPONSES,
  replayOpenCodeServer,
} from "./probeResponses.fixture.ts";
import { OpenCodeDriver, openCodeUpdateFor } from "./driver.ts";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";

const serverStarts: Array<string> = [];
const reachedServer = (operation: string) =>
  Effect.sync(() => serverStarts.push(operation)).pipe(
    Effect.andThen(
      Effect.fail(
        new OpenCodeRuntime.OpenCodeRuntimeError({
          operation,
          detail: "reached a 1.x server path",
        }),
      ),
    ),
  );
const openCode2Runtime = {
  runOpenCodeCommand: () => Effect.succeed({ stdout: "opencode v2.0.18\n", stderr: "", code: 0 }),
  startOpenCodeServerProcess: () => reachedServer("start"),
  connectToOpenCodeServer: () => reachedServer("connect"),
} as unknown as OpenCodeRuntime.OpenCodeRuntime["Service"];

const layer = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  ProviderLatestVersions.layer,
  TestProviderHost.layer(),
  Layer.succeed(
    ProviderEventLoggers.ProviderEventLoggers,
    ProviderEventLoggers.NoOpProviderEventLoggers,
  ),
  Layer.succeed(OpenCodeRuntime.OpenCodeRuntime, openCode2Runtime),
).pipe(Layer.provideMerge(NodeServices.layer));

const create = (config: Partial<OpenCodeSettings>, http: HttpClient.HttpClient) =>
  OpenCodeDriver.create({
    instanceId: ProviderInstanceId.make("opencode-test"),
    displayName: undefined,
    environment: [],
    enabled: true,
    config: { ...OpenCodeDriver.defaultConfig(), ...config },
  }).pipe(Effect.provideService(HttpClient.HttpClient, http));

const noHttp = HttpClient.make(() => Effect.die("A local binary must not be probed over HTTP"));

it.layer(layer)("OpenCodeDriver runtime selection", (it) => {
  it.effect("never starts a 1.x server for an OpenCode 2 instance", () =>
    Effect.gen(function* () {
      serverStarts.length = 0;
      const instance = yield* create({}, noHttp);
      yield* Effect.flip(
        instance.textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "hello",
          modelSelection: { instanceId: instance.instanceId, model: "opencode/big-pickle" },
        }),
      );
      assert.deepStrictEqual(serverStarts, ["start"]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "uses external model events for catalog readiness and keeps cached models without them",
    () =>
      Effect.gen(function* () {
        const host = yield* ProviderHost.ProviderHost;
        const secondModelRead = yield* Deferred.make<void>();
        const requestedPaths: Array<string> = [];
        let modelListReads = 0;
        let eventSubscriptions = 0;
        let integrationListReads = 0;
        const firstModel = {
          id: "big-pickle",
          modelID: "big-pickle",
          providerID: "opencode",
          name: "Big Pickle",
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          variants: [],
          time: { released: 0 },
          cost: [],
          status: "active",
          enabled: true,
          limit: { context: 200_000, output: 32_000 },
        };
        const laterModel = {
          ...firstModel,
          id: "later-model",
          modelID: "later-model",
          name: "Later Model",
        };
        const http = HttpClient.make((request) => {
          const url = new URL(request.url);
          requestedPaths.push(url.pathname);
          const location = {
            directory: url.searchParams.get("location[directory]") ?? "/work",
          };
          const json = (body: unknown, status = 200) =>
            HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify(body), {
                status,
                headers: { "content-type": "application/json" },
              }),
            );

          if (url.pathname === "/api/info") {
            return Effect.succeed(
              json({ version: "2.0.18", pid: 2393556, urls: [], paths: { tmp: "/tmp/opencode" } }),
            );
          }
          if (url.pathname === "/api/event") {
            eventSubscriptions += 1;
            // Only the first subscription sees the update. A warm server that
            // has already published its catalog will not emit another one, so
            // later subscriptions stay open and silent.
            const emitUpdate = eventSubscriptions === 1;
            const body = new ReadableStream<Uint8Array>({
              start(controller) {
                if (emitUpdate) {
                  const event = {
                    id: "evt_model-update",
                    created: 1,
                    type: "model.updated",
                    location: { directory: host.paths.cwd },
                    data: {},
                  };
                  controller.enqueue(
                    new TextEncoder().encode(`data: ${JSON.stringify(event)}\n\n`),
                  );
                  controller.close();
                }
              },
            });
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new Response(body, { headers: { "content-type": "text/event-stream" } }),
              ),
            );
          }
          if (url.pathname === "/api/model") {
            modelListReads += 1;
            // The first read is partial, the post-update read is complete, and
            // every later read repeats the complete catalog.
            const data = modelListReads === 1 ? [firstModel] : [firstModel, laterModel];
            const response = json({ location, data });
            return modelListReads === 2
              ? Deferred.succeed(secondModelRead, undefined).pipe(Effect.as(response))
              : Effect.succeed(response);
          }
          if (url.pathname === "/api/provider") {
            return Effect.succeed(json({ location, data: [] }));
          }
          if (url.pathname === "/api/integration") {
            integrationListReads += 1;
            return Effect.succeed(json({ _tag: "NotFoundError", message: "not recorded" }, 404));
          }
          return Effect.succeed(json({ _tag: "NotFoundError", message: "not recorded" }, 404));
        });
        const instance = yield* create(
          { serverUrl: "http://127.0.0.1:4096", serverPassword: "secret" },
          http,
        );
        const changed = yield* Stream.runHead(instance.snapshot.streamChanges).pipe(
          Effect.forkChild,
        );
        const reread = yield* Deferred.await(secondModelRead).pipe(
          Effect.timeoutOption("5 seconds"),
          Effect.map((result) => result._tag === "Some"),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        yield* TestClock.adjust("6 seconds");
        assert.isTrue(
          yield* Fiber.join(reread),
          `model.updated did not trigger a reread: ${requestedPaths}`,
        );

        const settledOption = yield* Fiber.join(changed);
        if (settledOption._tag === "None")
          throw new Error("OpenCode status did not publish a snapshot.");
        const settled = settledOption.value;
        assert.strictEqual(settled.status, "ready");
        assert.deepStrictEqual(
          settled.models.map((model) => model.slug),
          ["opencode/big-pickle", "opencode/later-model"],
        );
        assert.strictEqual(integrationListReads, 0);

        // The server already signalled readiness once, so a refresh must settle
        // immediately instead of waiting out the readiness timeout.
        const refreshed = yield* instance.snapshot.refresh;
        assert.strictEqual(refreshed.status, "ready");
        assert.deepStrictEqual(
          refreshed.models.map((model) => model.slug),
          settled.models.map((model) => model.slug),
        );
        assert.strictEqual(eventSubscriptions, 2);
        assert.strictEqual(modelListReads, 3);
        assert.strictEqual(integrationListReads, 0);
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("settles a stable catalog when the event feed ends before any update", () =>
    Effect.gen(function* () {
      let modelReads = 0;
      const http = HttpClient.make((request) => {
        const url = new URL(request.url);
        const location = {
          directory: url.searchParams.get("location[directory]") ?? "/work",
        };
        const json = (body: unknown, status = 200) =>
          HttpClientResponse.fromWeb(
            request,
            new Response(JSON.stringify(body), {
              status,
              headers: { "content-type": "application/json" },
            }),
          );
        if (url.pathname === "/api/info") {
          return Effect.succeed(
            json({ version: "2.0.18", pid: 2393556, urls: [], paths: { tmp: "/tmp/opencode" } }),
          );
        }
        if (url.pathname === "/api/event") {
          // A feed that ends at once can never deliver `model.updated`, so the
          // loader must give up rather than sit on the readiness timeout.
          const body = new ReadableStream<Uint8Array>({
            start(controller) {
              controller.close();
            },
          });
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(body, { headers: { "content-type": "text/event-stream" } }),
            ),
          );
        }
        if (url.pathname === "/api/model") {
          modelReads += 1;
          return Effect.succeed(
            json({
              location,
              data: [
                {
                  id: "big-pickle",
                  modelID: "big-pickle",
                  providerID: "opencode",
                  name: "Big Pickle",
                  capabilities: { tools: true, input: ["text"], output: ["text"] },
                  variants: [],
                  time: { released: 0 },
                  cost: [],
                  status: "active",
                  enabled: true,
                  limit: { context: 200_000, output: 32_000 },
                },
              ],
            }),
          );
        }
        if (url.pathname === "/api/provider") {
          return Effect.succeed(json({ location, data: [] }));
        }
        return Effect.succeed(json({ _tag: "NotFoundError", message: "not recorded" }, 404));
      });
      const instance = yield* create(
        { serverUrl: "http://127.0.0.1:4096", serverPassword: "secret" },
        http,
      );
      yield* Effect.yieldNow;
      const refresh = yield* instance.snapshot.refresh.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      // Far below the 30 second readiness timeout: settling inside this window
      // proves the abandoned feed did not leave the loader waiting.
      yield* TestClock.adjust("5 seconds");

      const settled = yield* Fiber.join(refresh);
      // The feed ended, so no event can arrive; the stable-catalog check settles
      // this instead of waiting out the readiness timeout.
      assert.strictEqual(settled.status, "ready");
      assert.deepStrictEqual(
        settled.models.map((model) => model.slug),
        ["opencode/big-pickle"],
      );
      // One boot probe plus this refresh, each reading twice: an initial read and
      // a confirming read. A different count means the settle path changed.
      assert.strictEqual(modelReads % 2, 0);
      assert.strictEqual(modelReads >= 2, true);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("gives up on an event endpoint that never answers", () =>
    Effect.gen(function* () {
      const http = HttpClient.make((request) => {
        const url = new URL(request.url);
        const location = {
          directory: url.searchParams.get("location[directory]") ?? "/work",
        };
        const json = (body: unknown, status = 200) =>
          HttpClientResponse.fromWeb(
            request,
            new Response(JSON.stringify(body), {
              status,
              headers: { "content-type": "application/json" },
            }),
          );
        if (url.pathname === "/api/info") {
          return Effect.succeed(
            json({ version: "2.0.18", pid: 2393556, urls: [], paths: { tmp: "/tmp/opencode" } }),
          );
        }
        if (url.pathname === "/api/event") {
          // Accepts the request and then stalls before sending response headers,
          // so the subscription never yields a stream.
          return Effect.never;
        }
        if (url.pathname === "/api/model") {
          return Effect.succeed(json({ location, data: [] }));
        }
        if (url.pathname === "/api/provider") {
          return Effect.succeed(json({ location, data: [] }));
        }
        return Effect.succeed(json({ _tag: "NotFoundError", message: "not recorded" }, 404));
      });
      const instance = yield* create(
        { serverUrl: "http://127.0.0.1:4096", serverPassword: "secret" },
        http,
      );
      yield* Effect.yieldNow;
      const refresh = yield* instance.snapshot.refresh.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("40 seconds");

      const unsettled = yield* Fiber.join(refresh);
      assert.strictEqual(unsettled.status, "warning");
      assert.strictEqual(
        unsettled.message,
        "OpenCode 2 is running, but it did not list any models yet.",
      );
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("lists an OpenCode 2 workspace's own skills and commands from its server", () =>
    Effect.gen(function* () {
      serverStarts.length = 0;
      const requested: Array<string> = [];
      const server = replayOpenCodeServer(
        { ...OPENCODE_2_RESPONSES, ...OPENCODE_2_WORKSPACE_RESPONSES },
        "secret",
        requested,
      );
      const instance = yield* create(
        { serverUrl: "http://127.0.0.1:4096", serverPassword: "secret" },
        server,
      );

      const workspace = yield* instance.snapshotForCwd!("/work");
      assert.includeMembers(
        workspace.skills.map((skill) => skill.name),
        ["plum", "opencode"],
      );
      assert.deepStrictEqual(
        workspace.slashCommands.map((command) => command.name),
        ["compact", "init", "review", "bee"],
      );
      assert.include(requested, "/api/skill");
      assert.deepStrictEqual(serverStarts, []);
    }).pipe(Effect.scoped),
  );

  it.effect("answers capability reads without waiting on an unreachable server", () =>
    Effect.gen(function* () {
      const hang = HttpClient.make(() => Effect.never);
      const instance = yield* create({ serverUrl: "http://127.0.0.1:9" }, hang);

      const capabilities = yield* instance.orchestrationAdapter
        .getCapabilities()
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isDefined(capabilities.pollUnsafe());
      yield* Fiber.join(capabilities);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );
});

// npm links `<prefix>/bin/opencode` into the package that installed it:
// `opencode-ai` for 1.x and `@opencode/cli` (whose bin is `bin/opencode.exe`) for 2.x.
const npmGlobalInstall = (prefix: string, packagePath: ReadonlyArray<string>, bin: string) => {
  const target = NodePath.join(prefix, "lib", "node_modules", ...packagePath, "bin", bin);
  NodeFS.mkdirSync(NodePath.dirname(target), { recursive: true });
  NodeFS.writeFileSync(target, "#!/bin/sh\n");
  NodeFS.chmodSync(target, 0o755);
  const link = NodePath.join(prefix, "bin", "opencode");
  NodeFS.mkdirSync(NodePath.dirname(link), { recursive: true });
  NodeFS.symlinkSync(target, link);
  return link;
};
const resolveUpdate = (generation: "v1" | "v2", binaryPath: string) =>
  ProviderMaintenance.resolveProviderMaintenanceCapabilitiesEffect(openCodeUpdateFor(generation), {
    binaryPath,
    env: { PATH: "" },
  }).pipe(
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.die("resolving an update must not spawn")),
    ),
    Effect.provide(NodeServices.layer),
  );

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "updates an npm-global OpenCode 2 install as @opencode/cli and a 1.x one as opencode-ai",
  () =>
    Effect.gen(function* () {
      const root = NodeFS.realpathSync(
        NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-opencode-update-")),
      );
      const v2Prefix = NodePath.join(root, "v2");
      const v1Prefix = NodePath.join(root, "v1");
      const v2 = yield* resolveUpdate(
        "v2",
        npmGlobalInstall(v2Prefix, ["@opencode", "cli"], "opencode.exe"),
      );
      assert.deepStrictEqual(v2.update?.args, [
        "install",
        "-g",
        "--prefix",
        v2Prefix,
        "--allow-scripts=@opencode/cli",
        "@opencode/cli@latest",
      ]);
      const v1 = yield* resolveUpdate(
        "v1",
        npmGlobalInstall(v1Prefix, ["opencode-ai"], "opencode"),
      );
      assert.strictEqual(v1.update?.args.at(-1), "opencode-ai@latest");
      assert.isNull(
        (yield* resolveUpdate("v2", NodePath.join(v1Prefix, "bin", "opencode"))).update,
      );
      assert.isNull(
        (yield* resolveUpdate("v1", NodePath.join(v2Prefix, "bin", "opencode"))).update,
      );
    }),
);

const versionOutput: { current: string | undefined } = { current: undefined };
const versionProbes: Array<string> = [];
const changingRuntime = {
  runOpenCodeCommand: ({ binaryPath }: { readonly binaryPath: string }) =>
    Effect.sync(() => versionProbes.push(binaryPath)).pipe(
      Effect.andThen(
        versionOutput.current === undefined
          ? Effect.fail(
              new OpenCodeRuntime.OpenCodeRuntimeError({
                operation: "version",
                detail: "no version",
              }),
            )
          : Effect.succeed({ stdout: versionOutput.current, stderr: "", code: 0 }),
      ),
    ),
  startOpenCodeServerProcess: () => reachedServer("start"),
  connectToOpenCodeServer: () => reachedServer("connect"),
} as unknown as OpenCodeRuntime.OpenCodeRuntime["Service"];
const layerUpdate = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  ProviderLatestVersions.layer,
  TestProviderHost.layer(),
  Layer.succeed(
    ProviderEventLoggers.ProviderEventLoggers,
    ProviderEventLoggers.NoOpProviderEventLoggers,
  ),
  Layer.succeed(OpenCodeRuntime.OpenCodeRuntime, changingRuntime),
).pipe(Layer.provideMerge(NodeServices.layer));

it.layer(layerUpdate)("OpenCodeDriver updates", (it) => {
  it.effect("never runs the binary for a disabled instance's update check", () =>
    Effect.gen(function* () {
      versionProbes.length = 0;
      versionOutput.current = "opencode v2.0.18\n";
      const instance = yield* OpenCodeDriver.create({
        instanceId: ProviderInstanceId.make("opencode-disabled"),
        displayName: undefined,
        environment: [],
        enabled: false,
        config: { ...OpenCodeDriver.defaultConfig(), binaryPath: "opencode" },
      }).pipe(Effect.provideService(HttpClient.HttpClient, noHttp));
      const maintenance = yield* instance.snapshot.resolveMaintenance();
      yield* instance.snapshot.resolveMaintenance({ fresh: true });
      assert.isNull(maintenance.update);
      assert.deepStrictEqual(versionProbes, []);
    }).pipe(Effect.scoped),
  );

  it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "offers no package update for an unknown version and follows a changed one on a fresh read",
    () =>
      Effect.gen(function* () {
        const root = NodeFS.realpathSync(
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-opencode-driver-update-")),
        );
        const binaryPath = npmGlobalInstall(NodePath.join(root, "v1"), ["opencode-ai"], "opencode");
        const instance = yield* OpenCodeDriver.create({
          instanceId: ProviderInstanceId.make("opencode-update"),
          displayName: undefined,
          environment: [],
          enabled: true,
          config: { ...OpenCodeDriver.defaultConfig(), binaryPath },
        }).pipe(Effect.provideService(HttpClient.HttpClient, noHttp));

        versionOutput.current = undefined;
        const unknown = yield* instance.snapshot.resolveMaintenance({ fresh: true });
        assert.isNull(unknown.update);
        assert.isNull(unknown.packageName);

        versionOutput.current = "1.18.32\n";
        const v1 = yield* instance.snapshot.resolveMaintenance({ fresh: true });
        assert.strictEqual(v1.packageName, "opencode-ai");

        versionOutput.current = "opencode v2.0.18\n";
        const v2 = yield* instance.snapshot.resolveMaintenance({ fresh: true });
        assert.strictEqual(v2.packageName, "@opencode/cli");
        assert.isNull(v2.update);
      }).pipe(Effect.scoped),
  );
});
