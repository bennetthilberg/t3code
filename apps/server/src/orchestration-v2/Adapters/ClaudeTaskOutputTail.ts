import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { tailTaskOutputFile } from "./TaskOutputTail.ts";

/** The SDK withholds stdout until tool results; tail task_started files for live output.
 * The private Claude layout can change; failures omit live output and retain the final result. */

/** Claude Code's per-user temp root, or none where it has no uid (Windows). */
export function claudeTaskOutputRoot(
  environment: Readonly<Record<string, string | undefined>> | undefined,
  path: Path.Path,
  cwd: string | null,
  platform: NodeJS.Platform,
): string | undefined {
  const uid = process.getuid?.();
  if (uid === undefined) return undefined;
  // Claude Code's macOS build falls back to /tmp and ignores the TMPDIR that
  // launchd always sets. Its other builds use `os.tmpdir()`, which reads these
  // from the environment it was launched with. A relative value is relative
  // to Claude's working directory, not this server's.
  const fallback =
    platform === "darwin"
      ? "/tmp"
      : environment?.TMPDIR || environment?.TMP || environment?.TEMP || "/tmp";
  const base = environment?.CLAUDE_CODE_TMPDIR?.trim() || fallback;
  return path.resolve(cwd ?? ".", base, `claude-${uid}`);
}

const locate = Effect.fn("ClaudeTaskOutputTail.locate")(function* (input: {
  readonly root: string;
  readonly sessionId: string;
  readonly taskId: string;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projects = yield* fileSystem.readDirectory(input.root).pipe(Effect.orElseSucceed(() => []));
  for (const project of projects) {
    const candidate = path.join(
      input.root,
      project,
      input.sessionId,
      "tasks",
      `${input.taskId}.output`,
    );
    if (yield* fileSystem.exists(candidate).pipe(Effect.orElseSucceed(() => false))) {
      return Option.some(candidate);
    }
  }
  return Option.none<string>();
});

/** Complete stop on tool result or turn end, then await the effect; it performs a final read before returning. */
export const tailClaudeTaskOutput = Effect.fn("ClaudeTaskOutputTail.tail")(function* (input: {
  readonly root: string;
  readonly sessionId: string;
  readonly taskId: string;
  readonly stop: Deferred.Deferred<void>;
  readonly onChunk: (chunk: string) => Effect.Effect<void>;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* tailTaskOutputFile({
    locate: locate(input).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    ),
    stop: input.stop,
    onChunk: input.onChunk,
  });
});
