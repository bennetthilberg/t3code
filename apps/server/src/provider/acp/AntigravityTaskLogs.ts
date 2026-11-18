// @effect-diagnostics nodeBuiltinImport:off -- lstat proves a task log is the agent's own file.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { tailTaskOutputFile } from "../../orchestration-v2/Adapters/TaskOutputTail.ts";
import { antigravityManagedPath, antigravityProfileRoot } from "./AntigravityProfilePaths.ts";

const NATIVE_SESSION_ID = /^[\w-]+$/;
const TASK_LOG = /^task-(\d+)\.log$/;
const MAX_CLAIMED_LOGS = 256;
// File timestamps can trail the server clock by a filesystem tick.
const TIMESTAMP_SLACK_MS = 2_000;

interface WaitingCommand {
  readonly startedAt: number;
}

/**
 * Antigravity writes a backgrounded `run_command`'s whole output to
 * `brain/<session>/.system_generated/tasks/task-<step>.log`, but names that log only once the
 * command ends. Steps number calls in order and commands background in that order, so a
 * session's Nth waiting command takes the Nth unclaimed log written since it started.
 */
export function makeAntigravityTaskLogs(profileDirectory: string) {
  const claimed = new Set<string>();
  const waiting = new Map<string, Array<WaitingCommand>>();

  const unclaimedLogs = (sessionId: string, since: number) =>
    Effect.tryPromise({
      try: async () => {
        const root = await antigravityProfileRoot(profileDirectory);
        if (root === undefined) return [];
        const generated = ["antigravity-acp", "brain", sessionId, ".system_generated"];
        const tasks = await antigravityManagedPath(root, [...generated, "tasks"], "directory");
        if (tasks === undefined) return [];
        const logs: Array<{ readonly file: string; readonly step: number }> = [];
        for (const name of await NodeFSP.readdir(tasks.path)) {
          const step = TASK_LOG.exec(name)?.[1];
          if (step === undefined) continue;
          const file = NodePath.join(tasks.path, name);
          const info = await NodeFSP.lstat(file).catch(() => undefined);
          if (info?.isFile() !== true || info.mtimeMs < since) continue;
          // A step's output file appears when its command ends. A finished command needs no tail,
          // and its log, left unclaimed, would take a waiting command's place in the order.
          const output = NodePath.join(root, ...generated, "steps", step, "output.txt");
          if (
            await NodeFSP.lstat(output).then(
              () => true,
              () => false,
            )
          )
            continue;
          logs.push({ file, step: Number(step) });
        }
        return logs.toSorted((left, right) => left.step - right.step).map((log) => log.file);
      },
      catch: () => undefined,
    }).pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));

  const locate = Effect.fn("AntigravityTaskLogs.locate")(function* (
    sessionId: string,
    command: WaitingCommand,
  ) {
    const oldest = waiting.get(sessionId)?.[0]?.startedAt ?? command.startedAt;
    const logs = yield* unclaimedLogs(sessionId, oldest - TIMESTAMP_SLACK_MS);
    // Ranked after the read so a log another command claimed meanwhile is skipped.
    const queue = waiting.get(sessionId) ?? [];
    const rank = queue.indexOf(command);
    const file = logs.filter((log) => !claimed.has(log))[rank];
    if (rank === -1 || file === undefined) return Option.none<string>();
    claimed.add(file);
    for (const old of claimed) {
      if (claimed.size <= MAX_CLAIMED_LOGS) break;
      claimed.delete(old);
    }
    queue.splice(rank, 1);
    return Option.some(file);
  });

  const enqueue = (sessionId: string, command: WaitingCommand) => {
    const queue = waiting.get(sessionId) ?? [];
    const index = queue.findIndex((other) => other.startedAt > command.startedAt);
    queue.splice(index === -1 ? queue.length : index, 0, command);
    waiting.set(sessionId, queue);
  };

  const dequeue = (sessionId: string, command: WaitingCommand) => {
    const queue = waiting.get(sessionId);
    if (queue === undefined) return;
    const index = queue.indexOf(command);
    if (index !== -1) queue.splice(index, 1);
    if (queue.length === 0) waiting.delete(sessionId);
  };

  return (input: {
    readonly sessionId: string;
    readonly startedAt: number;
    readonly stop: Deferred.Deferred<void>;
    readonly onChunk: (chunk: string) => Effect.Effect<void>;
  }) => {
    if (!NATIVE_SESSION_ID.test(input.sessionId)) return Effect.void;
    const command: WaitingCommand = { startedAt: input.startedAt };
    return Effect.sync(() => enqueue(input.sessionId, command)).pipe(
      Effect.andThen(
        tailTaskOutputFile({
          locate: locate(input.sessionId, command),
          stop: input.stop,
          onChunk: input.onChunk,
        }),
      ),
      Effect.ensuring(Effect.sync(() => dequeue(input.sessionId, command))),
    );
  };
}
