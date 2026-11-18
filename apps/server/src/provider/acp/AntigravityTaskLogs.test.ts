import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";

import { makeAntigravityTaskLogs } from "./AntigravityTaskLogs.ts";

const SESSION_ID = "ac8a998f-e353-44f9-955d-ab23c8f68033";

const makeProfile = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profile = yield* fileSystem.makeTempDirectoryScoped({ prefix: "antigravity-task-logs-" });
  const tasks = (sessionId: string) =>
    path.join(profile, "antigravity-acp", "brain", sessionId, ".system_generated", "tasks");
  yield* fileSystem.makeDirectory(tasks(SESSION_ID), { recursive: true });
  return { profile: yield* fileSystem.realPath(profile), tasks, log: path.join };
});

// TestClock cannot interleave with real filesystem I/O, so these use the live clock.
describe("makeAntigravityTaskLogs", () => {
  it.live("gives each backgrounded command its own new log, in call order", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const { profile, tasks, log } = yield* makeProfile;
        const now = yield* Clock.currentTimeMillis;
        // A log from before these commands and one from another session are never theirs.
        const stale = log(tasks(SESSION_ID), "task-1.log");
        yield* fileSystem.writeFileString(stale, "stale\n");
        // Node reads numeric file times as seconds.
        const minuteAgo = (now - 60_000) / 1000;
        yield* fileSystem.utimes(stale, minuteAgo, minuteAgo);
        yield* fileSystem.makeDirectory(tasks("other-session"), { recursive: true });
        yield* fileSystem.writeFileString(log(tasks("other-session"), "task-9.log"), "other\n");

        const tail = makeAntigravityTaskLogs(profile);
        const follow = (startedAt: number) =>
          Effect.gen(function* () {
            const chunks = yield* Queue.unbounded<string>();
            const stop = yield* Deferred.make<void>();
            const fiber = yield* tail({
              sessionId: SESSION_ID,
              startedAt,
              stop,
              onChunk: (chunk) => Queue.offer(chunks, chunk),
            }).pipe(Effect.forkScoped);
            return { chunks, stop, fiber };
          });
        const first = yield* follow(now - 2_000);
        const second = yield* follow(now - 1_000);

        const firstLog = log(tasks(SESSION_ID), "task-4.log");
        yield* fileSystem.writeFileString(firstLog, "first 0\r\n");
        yield* fileSystem.writeFileString(log(tasks(SESSION_ID), "task-12.log"), "second 0\r\n");
        assert.equal(yield* Queue.take(first.chunks), "first 0\r\n");
        assert.equal(yield* Queue.take(second.chunks), "second 0\r\n");

        // The log keeps growing while the command runs; a stop reads what landed last.
        yield* fileSystem.writeFileString(firstLog, "first 0\r\nfirst 1\r\n");
        assert.equal(yield* Queue.take(first.chunks), "first 1\r\n");
        yield* fileSystem.writeFileString(firstLog, "first 0\r\nfirst 1\r\nfirst 2\r\n");
        yield* Deferred.succeed(first.stop, undefined);
        yield* Fiber.join(first.fiber);
        assert.deepEqual(yield* Queue.takeAll(first.chunks), ["first 2\r\n"]);
        yield* Deferred.succeed(second.stop, undefined);
        yield* Fiber.join(second.fiber);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("ignores a linked log, which could be another session's file", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const { profile, tasks, log } = yield* makeProfile;
        const elsewhere = log(profile, "elsewhere.log");
        yield* fileSystem.writeFileString(elsewhere, "secret\n");
        yield* fileSystem.symlink(elsewhere, log(tasks(SESSION_ID), "task-2.log"));
        const chunks: Array<string> = [];
        const stop = yield* Deferred.make<void>();
        yield* Deferred.succeed(stop, undefined);
        yield* makeAntigravityTaskLogs(profile)({
          sessionId: SESSION_ID,
          startedAt: 0,
          stop,
          onChunk: (chunk) => Effect.sync(() => chunks.push(chunk)),
        });
        assert.deepEqual(chunks, []);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
