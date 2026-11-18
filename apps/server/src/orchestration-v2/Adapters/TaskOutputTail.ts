import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";

/** Follows the output file an agent writes for a command and withholds from the wire.
 * The file layouts are private to each agent; failures omit live output and keep the final result. */

const TASK_OUTPUT_POLL_MS = 250;
/** Bound discovery when the agent never writes the file. */
const MAX_LOCATE_ATTEMPTS = 40;
/** Skip ahead of fast writers; only the tail is displayed. */
const MAX_READ_BYTES = 256 * 1024;

/** Complete stop on tool result or turn end, then await the effect; it performs a final read before returning. */
export const tailTaskOutputFile = Effect.fn("TaskOutputTail.tail")(function* (input: {
  readonly locate: Effect.Effect<Option.Option<string>>;
  readonly stop: Deferred.Deferred<void>;
  readonly onChunk: (chunk: string) => Effect.Effect<void>;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const pause = Deferred.await(input.stop).pipe(
    Effect.as(true),
    Effect.timeoutOrElse({
      duration: TASK_OUTPUT_POLL_MS,
      orElse: () => Effect.succeed(false),
    }),
  );
  let file = Option.none<string>();
  for (let attempt = 0; Option.isNone(file); attempt += 1) {
    file = yield* input.locate;
    if (Option.isSome(file)) break;
    if (attempt + 1 >= MAX_LOCATE_ATTEMPTS || (yield* pause)) return;
  }
  const filePath = file.value;
  let decoder = new TextDecoder("utf-8", { fatal: false });
  let offset = 0;
  let last = yield* Deferred.isDone(input.stop);
  while (true) {
    const size = yield* fileSystem.stat(filePath).pipe(
      Effect.map((info) => Number(info.size)),
      Effect.orElseSucceed(() => offset),
    );
    if (size < offset) {
      offset = 0;
      decoder = new TextDecoder("utf-8", { fatal: false });
    }
    if (size > offset) {
      const start = size - offset > MAX_READ_BYTES ? size - MAX_READ_BYTES : offset;
      const bytes = yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* fileSystem.open(filePath, { flag: "r" });
          yield* handle.seek(BigInt(start), "start");
          return yield* handle.readAlloc(size - start);
        }),
      ).pipe(Effect.orElseSucceed(() => Option.none<Uint8Array>()));
      if (Option.isSome(bytes)) {
        // Skipped output must not splice two unrelated lines together.
        const skipped = start > offset;
        let first = 0;
        if (skipped) {
          // Reset partial UTF-8 bytes, and skip the rest of a code point the cut landed inside.
          decoder = new TextDecoder("utf-8", { fatal: false });
          while (((bytes.value[first] ?? 0) & 0xc0) === 0x80) first += 1;
        }
        offset = start + bytes.value.byteLength;
        const text =
          (skipped ? "\n" : "") + decoder.decode(bytes.value.subarray(first), { stream: true });
        if (text.length > 0) yield* input.onChunk(text);
      }
    }
    if (last) {
      const rest = decoder.decode();
      if (rest.length > 0) yield* input.onChunk(rest);
      return;
    }
    last = yield* pause;
  }
});
