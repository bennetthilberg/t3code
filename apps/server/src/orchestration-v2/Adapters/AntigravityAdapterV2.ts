import {
  ANTIGRAVITY_DEFAULT_MODEL,
  ProviderDriverKind,
  ProviderInstanceId,
  type OrchestrationV2ProviderCapabilities,
  type ProviderSetupError,
} from "@t3tools/contracts";
import type { SelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Exit from "effect/Exit";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as EffectAcpErrors from "effect-acp/errors";
import * as EffectAcpSchemaV1 from "effect-acp/schema-v1";
import type * as EffectAcpSchema from "effect-acp/compat";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { PULL_REQUEST_LINKING_INSTRUCTIONS } from "@t3tools/provider-core/server/runtimeInstructions";
import type { AntigravityAuth } from "../../provider/AntigravityAuth.ts";
import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import {
  antigravityAppendsSystemPrompt,
  antigravityPermissionMode,
  antigravityRuntimeModeForSessionMode,
  applyAntigravityAcpModelSelection,
  buildAntigravityPrompt,
  type AntigravityAcpRuntimeInput,
} from "../../provider/acp/AntigravityAcpSupport.ts";
import {
  ANTIGRAVITY_SUBAGENT_BATCH_TITLE,
  antigravitySubagentLaunches,
  describeAntigravitySubagentLaunches,
} from "../../provider/acp/AntigravityChildWork.ts";
import { readAntigravityClientTextFile } from "../../provider/acp/AntigravityClientFiles.ts";
import { makeAntigravityTaskLogs } from "../../provider/acp/AntigravityTaskLogs.ts";
import { watchAntigravityPromptOutcome } from "../../provider/acp/AntigravityTurnOutcome.ts";
import {
  antigravityApprovalOptions,
  antigravitySubagentOutput,
  classifyAntigravitySubagentToolCall,
  extractAntigravityUserInputQuestion,
  makeAntigravityBackgroundCommands,
  makeAntigravityReadRepair,
  makeAntigravityUserInputResponse,
  normalizeAntigravityToolCall,
} from "../../provider/acp/AntigravityProtocol.ts";
import { isAntigravitySignInRequiredError } from "../../provider/antigravityAuthSupport.ts";
import {
  AcpProviderCapabilitiesV2,
  makeAcpAdapterV2,
  type AcpAdapterV2Flavor,
  type AcpAdapterV2RuntimeInput,
} from "@t3tools/provider-acp/server/adapter";

const ANTIGRAVITY_PROVIDER = ProviderDriverKind.make("antigravity");

const AntigravityProviderCapabilitiesV2 = {
  ...AcpProviderCapabilitiesV2,
  sessions: {
    ...AcpProviderCapabilitiesV2.sessions,
    supportsModelSwitchInSession: true,
    supportsRuntimeModeSwitchInSession: true,
  },
  turns: {
    ...AcpProviderCapabilitiesV2.turns,
    supportsActiveSteering: true,
    activeSteeringInterruptsTools: false,
  },
  tools: {
    ...AcpProviderCapabilitiesV2.tools,
    supportsMcpTools: true,
  },
  subagents: {
    ...AcpProviderCapabilitiesV2.subagents,
    supportsSubagents: true,
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface AntigravityAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly selfInvocation: SelfInvocation;
  readonly makeRuntime: (
    input: Omit<AntigravityAcpRuntimeInput, "spawn" | "childProcessSpawner">,
  ) => Effect.Effect<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    EffectAcpErrors.AcpError | ProviderSetupError,
    Scope.Scope
  >;
  readonly withProcess: AntigravityAuth["withProcess"];
  readonly defaultModel: Effect.Effect<string | undefined>;
  readonly supportsNativeGoals?: Effect.Effect<boolean>;
  readonly onSessionStarted?: (
    started: AcpSessionRuntime.AcpSessionRuntimeStartResult,
    cwd: string,
  ) => Effect.Effect<void>;
  readonly onSessionEvent?: AcpAdapterV2Flavor["onSessionEvent"];
  readonly nativeLogging?: Parameters<typeof makeAcpAdapterV2>[0]["nativeLogging"];
  readonly continuationRequests?: Parameters<typeof makeAcpAdapterV2>[0]["continuationRequests"];
  /** The agent's GEMINI_HOME. Its session logs show turns that ended on an upstream error. */
  readonly profileDirectory?: string;
}

/**
 * Native subagent tools report no child session ids, so children stay read-only.
 * Only permission prompts carry the launch; NativeChildWorkQuery matches their ids by this text.
 */
const extractAntigravitySubagentUpdate: NonNullable<AcpAdapterV2Flavor["extractSubagentUpdate"]> = (
  toolCall,
) => {
  if (classifyAntigravitySubagentToolCall(toolCall) !== "subagent") {
    return undefined;
  }
  const status =
    toolCall.status === "failed" ? "failed" : toolCall.status === "pending" ? "pending" : "running";
  const launches = antigravitySubagentLaunches(toolCall.data.rawInput);
  const launch = launches === undefined ? undefined : describeAntigravitySubagentLaunches(launches);
  return {
    nativeTaskId: toolCall.toolCallId,
    prompt:
      launch?.prompt ??
      antigravitySubagentOutput(toolCall) ??
      toolCall.detail ??
      toolCall.title ??
      ANTIGRAVITY_SUBAGENT_BATCH_TITLE,
    title: launch?.title ?? ANTIGRAVITY_SUBAGENT_BATCH_TITLE,
    model: null,
    status,
    childSessionId: null,
    result: status === "failed" ? (antigravitySubagentOutput(toolCall) ?? null) : null,
  };
};

// Native subagents run hidden inside the agent, with no row, status or Stop in T3.
const ANTIGRAVITY_T3_INSTRUCTIONS_NOTE = `## Antigravity in T3 Code

- Subagents: always \`delegate_task\`, also for your own provider and model, so the user can watch and stop each child in T3. Never \`invoke_subagent\`, \`start_subagent\`, \`swarm\` or \`run_workflow\` unless the user names one.
- A \`run_command\` that goes to the background is tracked by T3, and you are woken in a new run when it ends. Ending your turn after launching one is fine; do not poll it. One you leave running on purpose, such as a dev server or watcher: call \`t3_background_wait\` with its task id and \`waiting=false\`.`;

interface OpenAntigravityPrompt {
  steered: Deferred.Deferred<EffectAcpSchema.PromptResponse, EffectAcpErrors.AcpError> | undefined;
}

type PromptUsage = NonNullable<EffectAcpSchema.PromptResponse["usage"]>;

const addOptionalTokens = (left: number | null | undefined, right: number | null | undefined) =>
  left == null && right == null ? {} : { value: (left ?? 0) + (right ?? 0) };

// Each prompt reports only its own share of the turn's tokens.
function addPromptUsage(
  left: PromptUsage | null | undefined,
  right: PromptUsage | null | undefined,
): PromptUsage | null | undefined {
  if (left == null) return right;
  if (right == null) return left;
  const thought = addOptionalTokens(left.thoughtTokens, right.thoughtTokens);
  const cachedRead = addOptionalTokens(left.cachedReadTokens, right.cachedReadTokens);
  const cachedWrite = addOptionalTokens(left.cachedWriteTokens, right.cachedWriteTokens);
  return {
    ...right,
    totalTokens: left.totalTokens + right.totalTokens,
    inputTokens: left.inputTokens + right.inputTokens,
    outputTokens: left.outputTokens + right.outputTokens,
    ...("value" in thought ? { thoughtTokens: thought.value } : {}),
    ...("value" in cachedRead ? { cachedReadTokens: cachedRead.value } : {}),
    ...("value" in cachedWrite ? { cachedWriteTokens: cachedWrite.value } : {}),
  };
}

/** Each steer ends the prompt before it with `end_turn`; the turn ends with the last one. */
const awaitSteeredPrompts = (
  open: OpenAntigravityPrompt,
  response: EffectAcpSchema.PromptResponse,
): Effect.Effect<EffectAcpSchema.PromptResponse, EffectAcpErrors.AcpError> => {
  const steered = open.steered;
  if (steered === undefined) return Effect.succeed(response);
  open.steered = undefined;
  return Deferred.await(steered).pipe(
    Effect.flatMap((next) => {
      const usage = addPromptUsage(response.usage, next.usage);
      return awaitSteeredPrompts(open, usage == null ? next : { ...next, usage });
    }),
  );
};

const decodePromptResponse = Schema.decodeUnknownEffect(EffectAcpSchemaV1.PromptResponse);

const decodeSteeredPromptResponse = (response: unknown) =>
  decodePromptResponse(response).pipe(
    Effect.mapError(
      (cause) =>
        new EffectAcpErrors.AcpRequestError({
          code: -32603,
          errorMessage: "Antigravity answered a steered prompt in an unknown shape.",
          method: "session/prompt",
          data: response,
          cause,
        }),
    ),
  );

interface AntigravityFlavorServices {
  readonly random: SecureRandom.SecureRandom["Service"];
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly host: ProviderHost.ProviderHost["Service"];
}

export function makeAntigravityAcpAdapterFlavor(
  options: AntigravityAdapterV2Options & AntigravityFlavorServices,
): AcpAdapterV2Flavor {
  const { profileDirectory } = options;
  const tailTaskLog =
    profileDirectory === undefined ? undefined : makeAntigravityTaskLogs(profileDirectory);
  // Each runtime tracks its own open commands; their ids are unique, so the result is shared.
  const backgroundedCommandIds = new Set<string>();
  const backgroundCommandTaskId = (toolCall: { readonly toolCallId: string }) =>
    backgroundedCommandIds.has(toolCall.toolCallId) ? toolCall.toolCallId : undefined;
  // Steering needs the runtime's own open prompt, so each wrapped runtime registers its sender.
  const steerers = new WeakMap<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    (sessionId: string, prompt: ReadonlyArray<EffectAcpSchema.ContentBlock>) => Effect.Effect<void>
  >();
  const antigravityClientFileRoots = (cwd: string | null) =>
    cwd === null ? [options.host.paths.attachmentsDir] : [cwd, options.host.paths.attachmentsDir];
  const makeRuntime = Effect.fn("AntigravityAdapterV2.makeRuntime")(
    function* (input: AcpAdapterV2RuntimeInput) {
      const scope = yield* Scope.fork(yield* Effect.scope);
      const runtime = yield* options
        .withProcess(
          Scope.close(scope, Exit.void),
          options.makeRuntime({
            ...input,
            clientFileReads: true,
            sessionSystemPromptAppend: PULL_REQUEST_LINKING_INSTRUCTIONS,
            // No added directories: the agent would run every command that names no `Cwd` there.
            // Attachment reads still work, as T3 serves them and allows read requests.
          }),
        )
        .pipe(Effect.provideService(Scope.Scope, scope));
      // Repair keeps state across updates, so each runtime must have one session-update handler.
      const reads = makeAntigravityReadRepair();
      const backgroundCommands = makeAntigravityBackgroundCommands(backgroundedCommandIds);
      // Served reads are reported from the read handler, so they take turns with agent updates.
      const updates = yield* Semaphore.make(1);
      let projectUpdate: Parameters<typeof runtime.handleSessionUpdate>[0] | undefined;
      // A prompt sent mid-turn ends the open prompt with `end_turn` and carries the turn on,
      // so the open prompt answers with the last steered prompt's response instead.
      let openPrompt: OpenAntigravityPrompt | undefined;
      const wrapped = {
        ...runtime,
        handleReadTextFile: (handler) =>
          runtime.handleReadTextFile((request, context) =>
            handler(request, context).pipe(
              Effect.onExit((exit) => {
                const served = reads.attempted(request.path, Exit.isSuccess(exit));
                if (served === undefined || projectUpdate === undefined) return Effect.void;
                return updates
                  .withPermit(
                    projectUpdate({
                      sessionId: served.sessionId,
                      update: {
                        sessionUpdate: "tool_call_update",
                        toolCallId: served.toolCallId,
                        status: "completed",
                      },
                    }),
                  )
                  .pipe(Effect.ignore);
              }),
            ),
          ),
        prompt: (payload, promptOptions) =>
          Effect.suspend(() => {
            const open: OpenAntigravityPrompt = { steered: undefined };
            openPrompt = open;
            reads.reset();
            return runtime.prompt(payload, promptOptions).pipe(
              Effect.flatMap((response) => awaitSteeredPrompts(open, response)),
              Effect.ensuring(
                Effect.sync(() => {
                  if (openPrompt === open) openPrompt = undefined;
                }),
              ),
            );
          }).pipe(
            // Marking and re-reporting share the update lock, so the adapter settles this turn
            // only after every backgrounded command is registered as one.
            Effect.tap(() =>
              updates.withPermit(
                Effect.suspend(() => {
                  const project = projectUpdate;
                  const marked = backgroundCommands.promptSettled();
                  return project === undefined
                    ? Effect.void
                    : Effect.forEach(marked, project, { discard: true });
                }),
              ),
            ),
          ),
        handleSessionUpdate: (handler) => {
          projectUpdate = handler;
          return runtime.handleSessionUpdate((notification) =>
            updates.withPermit(
              Effect.suspend(() => {
                const repaired = reads.repair(notification);
                const marked = backgroundCommands.observe(repaired);
                return Effect.forEach([...marked, repaired], handler, { discard: true });
              }),
            ),
          );
        },
        start: () =>
          runtime
            .start()
            .pipe(
              Effect.tap(
                (started) => options.onSessionStarted?.(started, input.cwd) ?? Effect.void,
              ),
            ),
      } satisfies typeof runtime;
      steerers.set(wrapped, (sessionId, prompt) =>
        Effect.gen(function* () {
          const steered = yield* Deferred.make<
            EffectAcpSchema.PromptResponse,
            EffectAcpErrors.AcpError
          >();
          // With no prompt open, as in a run the agent started itself, step ends settle the run.
          if (openPrompt !== undefined) openPrompt.steered = steered;
          yield* runtime.request("session/prompt", { sessionId, prompt }).pipe(
            Effect.flatMap(decodeSteeredPromptResponse),
            Effect.exit,
            Effect.flatMap((exit) => Deferred.done(steered, exit)),
            Effect.forkIn(scope),
          );
        }),
      );
      return wrapped;
    },
    Effect.provideService(SecureRandom.SecureRandom, options.random),
    Effect.mapError((cause): EffectAcpErrors.AcpError =>
      cause._tag === "ProviderSetupError"
        ? new EffectAcpErrors.AcpTransportError({ detail: cause.detail, cause })
        : cause,
    ),
  );

  return {
    driver: ANTIGRAVITY_PROVIDER,
    runtimeHarness: "Antigravity",
    capabilities: AntigravityProviderCapabilitiesV2,
    supportsNativeGoals: options.supportsNativeGoals ?? Effect.succeed(false),
    // Agents that advertise HTTP MCP skip the stdio bridge process; others keep stdio.
    mcpOverHttp: true,
    sessionCarriesPullRequestLinking: antigravityAppendsSystemPrompt,
    makeRuntime,
    buildAttachmentPrompt: ({
      text,
      attachments,
      resolveAttachmentPath,
      promptCapabilities,
      agentVersion,
      currentModel,
    }) =>
      buildAntigravityPrompt({
        input: text,
        attachments,
        resolveAttachmentPath,
        promptCapabilities,
        agentVersion,
        currentModel,
      }).pipe(
        Effect.map((blocks) => (text.trim() ? blocks.slice(1) : blocks)),
        Effect.provideService(FileSystem.FileSystem, options.fileSystem),
        Effect.provideService(Path.Path, options.path),
      ),
    // Loading history replays tool calls; resume avoids replay.
    subagentsIdleOnTurnCompletion: true,
    ...(options.onSessionEvent === undefined ? {} : { onSessionEvent: options.onSessionEvent }),
    applyModelSelection: Effect.fn("AntigravityAdapterV2.applyModelSelection")(function* ({
      runtime,
      modelSelection,
    }) {
      const defaultModel = yield* options.defaultModel;
      return yield* applyAntigravityAcpModelSelection({
        runtime,
        model:
          modelSelection.model === ANTIGRAVITY_DEFAULT_MODEL ? undefined : modelSelection.model,
        defaultModel,
        mapError: (cause) => cause,
      });
    }),
    sessionModeForPolicy: (policy) => antigravityPermissionMode(policy.runtimeMode),
    runtimeModeForSessionMode: antigravityRuntimeModeForSessionMode,
    clientFileSystem: {
      readTextFile: (request, cwd) =>
        readAntigravityClientTextFile({
          fileSystem: options.fileSystem,
          path: options.path,
          allowedRoots: antigravityClientFileRoots(cwd),
          request,
        }),
    },
    approvalOptions: antigravityApprovalOptions,
    extractPermissionQuestion: (request) => {
      const question = extractAntigravityUserInputQuestion(request);
      if (question === undefined) return undefined;
      return {
        question,
        respond: (answers) => makeAntigravityUserInputResponse(request, answers),
      };
    },
    normalizeToolCall: normalizeAntigravityToolCall,
    extractSubagentUpdate: extractAntigravitySubagentUpdate,
    extractBackgroundTaskId: backgroundCommandTaskId,
    isBackgroundCommandTool: (toolCall) => backgroundCommandTaskId(toolCall) !== undefined,
    // The agent reacts to every finished background task in a turn it starts itself.
    deferFinalizeForBackgroundWork: true,
    enablePostSettleContinuation: true,
    backgroundCommandsWakeAgent: true,
    // ACP v1 reactive turns have no end marker; each model step ends with a usage report.
    continuationStepEnded: (update) => update.sessionUpdate === "usage_update",
    continuationQuietFallback: "2 minutes",
    t3InstructionsNote: ANTIGRAVITY_T3_INSTRUCTIONS_NOTE,
    ...(tailTaskLog === undefined
      ? {}
      : {
          tailBackgroundCommandOutput: (input) =>
            tailTaskLog(input).pipe(
              Effect.provideService(FileSystem.FileSystem, options.fileSystem),
            ),
        }),
    steering: {
      send: (runtime, sessionId, text, images) => {
        const steer = steerers.get(runtime);
        return steer === undefined
          ? Effect.fail(
              new EffectAcpErrors.AcpRequestError({
                code: -32603,
                errorMessage: "This Antigravity runtime cannot steer.",
                method: "session/prompt",
              }),
            )
          : steer(sessionId, [{ type: "text", text }, ...images]).pipe(Effect.as(undefined));
      },
    },
    supportsImagePrompts: true,
    // ACP 1.3.0 treats /compact as ordinary model input.
    supportsCompaction: false,
    requiresSignIn: isAntigravitySignInRequiredError,
    // Antigravity can leave a prompt open without updates; long tools and approvals pause this.
    promptInactivityTimeout: "15 minutes",
    // 1.4.5 returns the root trajectory's Gemini usageMetadata delta; 1.4.1 returns none.
    reportsPerTurnPromptUsage: true,
    geminiPromptUsage: true,
    ...(profileDirectory === undefined
      ? {}
      : {
          watchPromptOutcome: (sessionId: string) =>
            watchAntigravityPromptOutcome(profileDirectory, sessionId),
        }),
  };
}

export const makeAntigravityAdapterV2 = Effect.fn("makeAntigravityAdapterV2")(function* (
  options: AntigravityAdapterV2Options,
) {
  const services: AntigravityFlavorServices = {
    random: yield* SecureRandom.SecureRandom,
    fileSystem: yield* FileSystem.FileSystem,
    path: yield* Path.Path,
    host: yield* ProviderHost.ProviderHost,
  };
  return yield* makeAcpAdapterV2({
    instanceId: options.instanceId,
    flavor: makeAntigravityAcpAdapterFlavor({ ...options, ...services }),
    selfInvocation: options.selfInvocation,
    ...(options.nativeLogging === undefined ? {} : { nativeLogging: options.nativeLogging }),
    ...(options.continuationRequests === undefined
      ? {}
      : { continuationRequests: options.continuationRequests }),
  });
});
