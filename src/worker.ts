import { PgmqError, toPgmqError } from "./errors.js";
import type { Executor, TransactionalExecutor } from "./executor.js";
import { toMessage } from "./parse.js";
import { decodeMessages, decodeMsgIds, type RawMessage } from "./rows.js";
import { sleep } from "./sleep.js";
import * as sql from "./sql.js";
import type { Message } from "./types.js";
import { encodeMsgId, validateInt, validateQueueName } from "./validate.js";

/** Passed to the handler along with each message. */
export interface HandlerContext {
  /**
   * Aborted if the worker is shutting down and its `shutdownTimeoutMs` runs out while this
   * handler is still running. Pass it to anything cancellable and stop early when it fires;
   * the message is then delivered again after its visibility timeout.
   */
  readonly signal: AbortSignal;
}

/**
 * Handles one message. Returning (or resolving) means success: the worker then deletes or
 * archives the message. Throwing (or rejecting) means failure: the message is retried.
 *
 * Delivery is at-least-once, so a handler can see the same message more than once (after a
 * crash, a timeout, or a failed acknowledgement). Make it idempotent.
 */
export type Handler<T> = (message: Message<T>, context: HandlerContext) => void | Promise<void>;

/**
 * What the worker was doing when an error happened:
 *
 * - `read`: reading from the queue failed (for example, the database is down). The worker
 *   waits `pollIntervalMs` and tries again.
 * - `handle`: the handler threw, or the message failed to parse. The message will be retried.
 * - `ack`: deleting or archiving a handled message failed. It will be delivered again.
 * - `retry`: setting the backoff delay failed. The message reappears after its visibility timeout.
 * - `dead-letter`: moving an exhausted message to the dead-letter queue (or the archive) failed.
 *   The move is tried again the next time the message is read.
 * - `shutdown`: handlers were still running when `shutdownTimeoutMs` ran out.
 */
export type WorkerStage = "read" | "handle" | "ack" | "retry" | "dead-letter" | "shutdown";

/** Passed to `onError` along with the error. */
export interface WorkerErrorContext {
  readonly stage: WorkerStage;
  /** The message involved, if the error concerns one. */
  readonly msgId?: bigint;
}

/** Exponential backoff between attempts, applied with `setVt` after a handler fails. */
export interface BackoffOptions {
  /** Delay after the first failed attempt, in whole seconds. Doubles after every attempt. */
  readonly baseSeconds: number;
  /** The longest delay, in whole seconds. */
  readonly maxSeconds: number;
}

/** Options for {@link Worker}. */
export interface WorkerOptions<T> {
  /** The queue to consume. */
  readonly queue: string;
  /** Called once per message. See {@link Handler}. */
  readonly handler: Handler<T>;
  /**
   * Visibility timeout in whole seconds. Must be longer than your slowest handler: once it runs
   * out, the message is delivered again, even if the first handler is still working on it.
   */
  readonly vt: number;
  /** Checks each message body before the handler sees it. See {@link QueueOptions.parse}. */
  readonly parse?: (raw: unknown) => T;
  /** The most messages handled at the same time. Default 1. */
  readonly concurrency?: number;
  /**
   * The most messages fetched by one read. At most `concurrency`, and never more than there are
   * free handler slots, so no message waits in memory while its visibility timeout runs.
   * Default: `concurrency`.
   */
  readonly batchSize?: number;
  /** Milliseconds to wait before reading again when the queue is empty. Default 1000. */
  readonly pollIntervalMs?: number;
  /**
   * What to do with a message after its handler succeeds: delete it, or move it to the queue's
   * archive table. Default `"delete"`.
   */
  readonly onSuccess?: "delete" | "archive";
  /**
   * How many times a message may be handled. When a message is read with a `readCount` above
   * this, the handler is not called and the message is dead-lettered instead. `Infinity` retries
   * forever. Default 5.
   */
  readonly maxAttempts?: number;
  /**
   * Where exhausted messages go. The worker sends the message (body and headers unchanged) to
   * this queue and deletes it from the source queue in one transaction. The queue must exist.
   * Without it, exhausted messages are archived in the source queue's archive table.
   */
  readonly deadLetterQueue?: string;
  /**
   * Delay retries with exponential backoff: after a failed attempt `n`, the message becomes
   * visible again after `min(maxSeconds, baseSeconds * 2^(n-1))` seconds. Without it, a failed
   * message is retried when its visibility timeout runs out.
   */
  readonly backoff?: BackoffOptions;
  /**
   * How long a shutdown waits for running handlers, in milliseconds. When it runs out, their
   * `signal` is aborted and `run()` resolves without waiting further. Default 30000.
   */
  readonly shutdownTimeoutMs?: number;
  /** Called for every error the worker handles itself. Default: logs with `console.error`. */
  readonly onError?: (error: unknown, context: WorkerErrorContext) => void;
}

/** Options for {@link Worker.run}. */
export interface RunOptions {
  /** Abort it to stop the worker gracefully. Without a signal, `run` never resolves. */
  readonly signal?: AbortSignal;
}

/**
 * Consumes a queue: reads messages, calls the handler for each, and acknowledges, retries, or
 * dead-letters them. Built only on the queue primitives (read, setVt, delete, archive, send).
 *
 * Delivery is at-least-once: every message is handled until a handler succeeds and the
 * acknowledgement commits, or until it runs out of attempts. Handlers must be idempotent.
 *
 * @example
 * const worker = new Worker(sql, {
 *   queue: "orders",
 *   parse: OrderPlaced.parse,
 *   vt: 30,
 *   concurrency: 4,
 *   deadLetterQueue: "orders_dlq",
 *   handler: async (message) => {
 *     await sendConfirmationEmail(message.message);
 *   },
 * });
 *
 * const controller = new AbortController();
 * process.once("SIGTERM", () => controller.abort());
 * await worker.run({ signal: controller.signal }); // resolves after a graceful shutdown
 */
export class Worker<T> {
  readonly #db: TransactionalExecutor;
  readonly #queue: string;
  readonly #handler: Handler<T>;
  readonly #parse: ((raw: unknown) => T) | undefined;
  readonly #vt: number;
  readonly #concurrency: number;
  readonly #batchSize: number;
  readonly #pollIntervalMs: number;
  readonly #onSuccess: "delete" | "archive";
  readonly #maxAttempts: number;
  readonly #deadLetterQueue: string | undefined;
  readonly #backoff: BackoffOptions | undefined;
  readonly #shutdownTimeoutMs: number;
  readonly #onError: (error: unknown, context: WorkerErrorContext) => void;
  #running = false;

  /**
   * @param executor A Bun `SQL` client (or a reserved connection). It must be able to `begin`
   *   transactions, which the worker uses to move messages to the dead-letter queue.
   */
  constructor(executor: TransactionalExecutor, options: WorkerOptions<T>) {
    if (typeof executor !== "function" || typeof executor.begin !== "function") {
      throw new TypeError("Worker needs a Bun SQL client or reserved connection: something with a begin() method");
    }
    if (typeof options.handler !== "function") {
      throw new TypeError("handler must be a function");
    }
    this.#db = executor;
    this.#queue = validateQueueName(options.queue);
    this.#handler = options.handler;
    this.#parse = options.parse;
    this.#vt = validateInt("vt", options.vt, 1);
    this.#concurrency = validateInt("concurrency", options.concurrency ?? 1, 1);
    this.#batchSize = validateInt("batchSize", options.batchSize ?? this.#concurrency, 1);
    if (this.#batchSize > this.#concurrency) {
      throw new RangeError(
        `batchSize (${this.#batchSize}) cannot be larger than concurrency (${this.#concurrency}): ` +
          "the worker never reads more messages than it can start handling",
      );
    }
    this.#pollIntervalMs = validateInt("pollIntervalMs", options.pollIntervalMs ?? 1000, 1);
    this.#onSuccess = options.onSuccess ?? "delete";
    if (this.#onSuccess !== "delete" && this.#onSuccess !== "archive") {
      throw new TypeError(`onSuccess must be "delete" or "archive", got ${String(this.#onSuccess)}`);
    }
    const maxAttempts = options.maxAttempts ?? 5;
    this.#maxAttempts = maxAttempts === Infinity ? Infinity : validateInt("maxAttempts", maxAttempts, 1);
    if (options.deadLetterQueue !== undefined) {
      this.#deadLetterQueue = validateQueueName(options.deadLetterQueue);
      if (this.#deadLetterQueue === this.#queue) {
        throw new TypeError("deadLetterQueue must be a different queue than queue");
      }
    }
    if (options.backoff !== undefined) {
      const baseSeconds = validateInt("backoff.baseSeconds", options.backoff.baseSeconds, 1);
      const maxSeconds = validateInt("backoff.maxSeconds", options.backoff.maxSeconds, baseSeconds);
      this.#backoff = { baseSeconds, maxSeconds };
    }
    this.#shutdownTimeoutMs = validateInt("shutdownTimeoutMs", options.shutdownTimeoutMs ?? 30_000, 0);
    this.#onError = options.onError ?? this.#logError;
  }

  /**
   * Runs the worker until `signal` aborts, then shuts down gracefully: it stops reading, waits
   * for running handlers (up to `shutdownTimeoutMs`), and resolves.
   *
   * Errors while consuming (a failed read, a failing handler) never reject `run`; they go to
   * `onError` and the worker carries on.
   *
   * @throws {Error} If this worker is already running.
   */
  async run(options: RunOptions = {}): Promise<void> {
    if (this.#running) {
      throw new Error(`The worker for queue "${this.#queue}" is already running`);
    }
    this.#running = true;
    try {
      await this.#consume(options.signal);
    } finally {
      this.#running = false;
    }
  }

  async #consume(signal: AbortSignal | undefined): Promise<void> {
    const inFlight = new Set<Promise<void>>();
    const handlers = new AbortController();

    // Resolves the current wait for a free slot. Set right before waiting, so no wake-up is
    // lost: a finished handler can only run its callback after the loop has started waiting.
    let wake: (() => void) | undefined;
    const wakeUp = (): void => {
      wake?.();
      wake = undefined;
    };
    signal?.addEventListener("abort", wakeUp);

    try {
      while (signal?.aborted !== true) {
        const free = this.#concurrency - inFlight.size;
        if (free === 0) {
          await new Promise<void>((resolve) => (wake = resolve));
          continue;
        }

        let messages: RawMessage[];
        try {
          messages = decodeMessages(await sql.read(this.#db, this.#queue, this.#vt, Math.min(this.#batchSize, free)));
        } catch (error) {
          this.#report(toPgmqError(error, "pgmq.read", this.#queue), { stage: "read" });
          await pause(this.#pollIntervalMs, signal);
          continue;
        }

        if (messages.length === 0) {
          await pause(this.#pollIntervalMs, signal);
          continue;
        }

        // Every message read is handled, even if the signal aborted during the read: these
        // messages are already invisible to other readers.
        for (const raw of messages) {
          const task = this.#process(raw, handlers.signal).finally(() => {
            inFlight.delete(task);
            wakeUp();
          });
          inFlight.add(task);
        }
      }
    } finally {
      signal?.removeEventListener("abort", wakeUp);
    }

    await this.#drain(inFlight, handlers);
  }

  /** Handles one message from start to finish. Never rejects: every failure goes to onError. */
  async #process(raw: RawMessage, signal: AbortSignal): Promise<void> {
    if (raw.readCount > this.#maxAttempts) {
      await this.#deadLetter(raw);
      return;
    }

    try {
      const message = toMessage(this.#queue, raw, this.#parse);
      await this.#handler(message, { signal });
    } catch (error) {
      this.#report(error, { stage: "handle", msgId: raw.msgId });
      await this.#retryLater(raw);
      return;
    }

    await this.#acknowledge(raw);
  }

  /**
   * Deletes or archives a handled message. A `false` result (no such message) is fine: the
   * handler may have acknowledged it itself, for example in the same transaction as its writes.
   */
  async #acknowledge(raw: RawMessage): Promise<void> {
    const id = encodeMsgId(raw.msgId);
    const operation = this.#onSuccess === "delete" ? "pgmq.delete" : "pgmq.archive";
    try {
      await (this.#onSuccess === "delete"
        ? sql.deleteOne(this.#db, this.#queue, id)
        : sql.archiveOne(this.#db, this.#queue, id));
    } catch (error) {
      this.#report(toPgmqError(error, operation, this.#queue), { stage: "ack", msgId: raw.msgId });
    }
  }

  /** Applies the backoff delay, if configured. Otherwise the visibility timeout already set by the read applies. */
  async #retryLater(raw: RawMessage): Promise<void> {
    if (this.#backoff === undefined) {
      return;
    }
    const { baseSeconds, maxSeconds } = this.#backoff;
    const delay = Math.min(maxSeconds, baseSeconds * 2 ** (raw.readCount - 1));
    try {
      await sql.setVt(this.#db, this.#queue, encodeMsgId(raw.msgId), delay);
    } catch (error) {
      this.#report(toPgmqError(error, "pgmq.set_vt", this.#queue), { stage: "retry", msgId: raw.msgId });
    }
  }

  /**
   * Moves an exhausted message out of the queue. With a dead-letter queue, the send to the
   * dead-letter queue and the delete from the source commit together, so the message is never
   * lost and never in both queues. Body and headers arrive exactly as stored.
   */
  async #deadLetter(raw: RawMessage): Promise<void> {
    const id = encodeMsgId(raw.msgId);
    const target = this.#deadLetterQueue;
    try {
      if (target === undefined) {
        await sql.archiveOne(this.#db, this.#queue, id);
        return;
      }
      await this.#db.begin(async (tx: Executor) => {
        const copied = decodeMsgIds(await sql.copyMessage(tx, this.#queue, id, target, this.#vt));
        // No copy means the message is already gone (another worker moved it): nothing to delete.
        if (copied.length > 0) {
          await sql.deleteOne(tx, this.#queue, id);
        }
      });
    } catch (error) {
      // The source queue was just read, so if a queue is missing it is the dead-letter queue.
      const failure =
        target === undefined
          ? toPgmqError(error, "pgmq.archive", this.#queue)
          : toPgmqError(error, `moving message ${raw.msgId} from "${this.#queue}" to the dead-letter queue`, target);
      this.#report(failure, { stage: "dead-letter", msgId: raw.msgId });
    }
  }

  /** Waits for running handlers, up to the shutdown timeout. */
  async #drain(inFlight: ReadonlySet<Promise<void>>, handlers: AbortController): Promise<void> {
    if (inFlight.size === 0) {
      return;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<true>((resolve) => {
      timer = setTimeout(() => resolve(true), this.#shutdownTimeoutMs);
    });
    const finished = Promise.all(inFlight).then(() => false);
    const expired = await Promise.race([finished, timedOut]);
    clearTimeout(timer);
    if (expired) {
      const reason = new PgmqError(
        `Shutdown timed out after ${this.#shutdownTimeoutMs} ms with ${inFlight.size} handler(s) still running ` +
          `on queue "${this.#queue}". Their messages will be delivered again after their visibility timeout.`,
      );
      handlers.abort(reason);
      this.#report(reason, { stage: "shutdown" });
    }
  }

  #report(error: unknown, context: WorkerErrorContext): void {
    try {
      this.#onError(error, context);
    } catch (onErrorFailure) {
      console.error(`pgmq-ts: onError threw while reporting a "${context.stage}" error`, onErrorFailure);
    }
  }

  readonly #logError = (error: unknown, context: WorkerErrorContext): void => {
    const message = context.msgId === undefined ? "" : ` (message ${context.msgId})`;
    console.error(`pgmq-ts worker on queue "${this.#queue}": ${context.stage} failed${message}:`, error);
  };
}

/** Sleeps, but returns early (without throwing) when the signal aborts. */
async function pause(ms: number, signal: AbortSignal | undefined): Promise<void> {
  try {
    await sleep(ms, signal);
  } catch {
    // Aborted: the loop checks the signal and stops.
  }
}
