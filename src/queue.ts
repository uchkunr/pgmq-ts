import { MessageParseError, toPgmqError } from "./errors.js";
import type { Executor } from "./executor.js";
import { decodeBigint, decodeBoolean, decodeMessages, decodeMetrics, decodeMsgIds, type RawMessage } from "./rows.js";
import * as sql from "./sql.js";
import type { Message, MessageHeaders, QueueMetrics } from "./types.js";
import {
  encodeHeaders,
  encodeJson,
  encodeMsgId,
  isRecord,
  validateInt,
  validateQueueName,
  validateSecondsOrDate,
} from "./validate.js";

/** Options for {@link Pgmq.queue}. */
export interface QueueOptions<T> {
  /**
   * Turns a message body read from the database into a `T`, or throws if it is not one.
   * Any `(raw: unknown) => T` works: a Zod schema's `.parse`, Valibot's `v.parser(schema)`,
   * or a hand-written check.
   *
   * Without `parse`, messages are typed `T` without being checked: you are vouching for
   * everything in the queue. Leave out both `parse` and the type argument to get `unknown`.
   */
  readonly parse?: (raw: unknown) => T;
}

/** Options for {@link Queue.send}. */
export interface SendOptions {
  /**
   * Keep the message invisible until this time: whole seconds from now, or an absolute `Date`.
   * Default 0 (visible immediately).
   */
  readonly delay?: number | Date;
  /** Headers to store with the message. Must be a plain JSON object. */
  readonly headers?: MessageHeaders;
}

/** Options for {@link Queue.read}. */
export interface ReadOptions {
  /**
   * Visibility timeout in whole seconds: how long the messages stay invisible to other readers.
   * If a message is not deleted or archived by then, it is delivered again.
   */
  readonly vt: number;
  /** The maximum number of messages to read. Default 1. */
  readonly qty?: number;
}

/**
 * A handle to one PGMQ queue, created by {@link Pgmq.queue}. It holds the queue name, the
 * executor that runs its queries, and the optional `parse` function. It is immutable and cheap:
 * {@link Queue.with} returns a copy bound to another executor, such as a transaction.
 *
 * `T` is the message body type. Bodies are stored as `jsonb`, so they go through
 * `JSON.stringify` on the way in: a `Date` comes back as a string, and `undefined` properties
 * disappear. Use `parse` to turn them back into what you sent.
 */
export class Queue<T> {
  /** The queue name. */
  readonly name: string;
  readonly #db: Executor;
  readonly #parse: ((raw: unknown) => T) | undefined;

  /** @internal Use {@link Pgmq.queue} to get a queue handle. */
  constructor(db: Executor, name: string, parse?: (raw: unknown) => T) {
    this.name = validateQueueName(name);
    this.#db = db;
    this.#parse = parse;
  }

  /**
   * Returns the same queue bound to another executor, so its operations run there.
   * This is how a send joins your transaction:
   *
   * @example
   * await sql.begin(async (tx) => {
   *   await tx`INSERT INTO orders ${tx(order)}`;
   *   await orders.with(tx).send({ orderId: order.id });
   * });
   */
  with(executor: Executor): Queue<T> {
    return new Queue(executor, this.name, this.#parse);
  }

  /**
   * Creates the queue (its message table and archive table). Does nothing if it already exists.
   * Wraps `pgmq.create`.
   */
  async create(): Promise<void> {
    await this.#run("pgmq.create", () => sql.create(this.#db, this.name));
  }

  /**
   * Creates the queue with an UNLOGGED message table. Does nothing if it already exists.
   * Wraps `pgmq.create_unlogged`.
   *
   * Unlogged tables are faster to write but are not crash-safe and are not replicated: after a
   * crash or failover, every message in the queue is gone. Sends are still transactional.
   */
  async createUnlogged(): Promise<void> {
    await this.#run("pgmq.create_unlogged", () => sql.createUnlogged(this.#db, this.name));
  }

  /**
   * Deletes the queue, its messages, and its archive. Wraps `pgmq.drop_queue`.
   *
   * @returns `true` if the queue was dropped, `false` if it did not exist.
   */
  async drop(): Promise<boolean> {
    return this.#run("pgmq.drop_queue", async () => decodeBoolean(await sql.dropQueue(this.#db, this.name), "dropped"));
  }

  /**
   * Deletes every message in the queue, visible or not. The archive is kept.
   * Wraps `pgmq.purge_queue`.
   *
   * @returns The number of messages deleted.
   * @throws {QueueNotFoundError} If the queue does not exist.
   */
  async purge(): Promise<bigint> {
    return this.#run("pgmq.purge_queue", async () => decodeBigint(await sql.purgeQueue(this.#db, this.name), "purged"));
  }

  /**
   * Returns a snapshot of the queue's size and message ages. Wraps `pgmq.metrics`.
   *
   * @throws {QueueNotFoundError} If the queue does not exist.
   */
  async metrics(): Promise<QueueMetrics> {
    return this.#run("pgmq.metrics", async () => {
      const [metrics] = decodeMetrics(await sql.metrics(this.#db, this.name));
      if (metrics === undefined) {
        throw new Error("pgmq.metrics returned no row");
      }
      return metrics;
    });
  }

  /**
   * Sends one message. Wraps `pgmq.send`.
   *
   * Run it on a transaction (`queue.with(tx).send(...)`) and the message is only visible to
   * readers once the transaction commits; if it rolls back, the message was never sent.
   *
   * @param message The message body. Anything `JSON.stringify` accepts.
   * @returns The new message's id.
   * @throws {QueueNotFoundError} If the queue does not exist.
   */
  async send(message: T, options: SendOptions = {}): Promise<bigint> {
    const body = encodeJson(message, "message");
    const headers = options.headers === undefined ? null : encodeHeaders(options.headers, "headers");
    const delay = validateSecondsOrDate("delay", options.delay ?? 0);
    return this.#run("pgmq.send", async () => {
      const [msgId] = decodeMsgIds(await sql.send(this.#db, this.name, body, headers, delay));
      if (msgId === undefined) {
        throw new Error("pgmq.send returned no message id");
      }
      return msgId;
    });
  }

  /**
   * Reads up to `qty` messages and makes them invisible to other readers for `vt` seconds.
   * Wraps `pgmq.read`. Returns immediately, with an empty array if no message is visible.
   *
   * Reading does not remove a message. Delete or archive it once it has been handled; otherwise
   * it is delivered again when the visibility timeout expires, with `readCount` one higher.
   *
   * @throws {QueueNotFoundError} If the queue does not exist.
   * @throws {MessageParseError} If `parse` rejects a message. The other messages of that read
   *   are not returned and become visible again after `vt` seconds.
   */
  async read(options: ReadOptions): Promise<Message<T>[]> {
    const vt = validateInt("vt", options.vt, 0);
    const qty = validateInt("qty", options.qty ?? 1, 1);
    const raw = await this.#run("pgmq.read", async () => decodeMessages(await sql.read(this.#db, this.name, vt, qty)));
    return raw.map((message) => this.#toMessage(message));
  }

  /**
   * Deletes a message for good. Wraps `pgmq.delete`.
   *
   * @returns `true` if the message was deleted, `false` if no message has that id.
   */
  async delete(msgId: bigint): Promise<boolean> {
    const id = encodeMsgId(msgId);
    return this.#run("pgmq.delete", async () => decodeBoolean(await sql.deleteOne(this.#db, this.name, id), "deleted"));
  }

  /**
   * Moves a message to the queue's archive table (`pgmq.a_<queue>`), where it is kept for
   * inspection but never delivered again. Wraps `pgmq.archive`.
   *
   * @returns `true` if the message was archived, `false` if no message has that id.
   */
  async archive(msgId: bigint): Promise<boolean> {
    const id = encodeMsgId(msgId);
    return this.#run("pgmq.archive", async () => decodeBoolean(await sql.archiveOne(this.#db, this.name, id), "archived"));
  }

  /** Runs one database operation and converts whatever it throws into a {@link PgmqError}. */
  async #run<R>(operation: string, fn: () => Promise<R> | PromiseLike<R>): Promise<R> {
    try {
      return await fn();
    } catch (error) {
      throw toPgmqError(error, operation, this.name);
    }
  }

  /** Checks the headers and parses the body of a message read from the database. */
  #toMessage(raw: RawMessage): Message<T> {
    const { headers } = raw;
    if (headers !== null && !isRecord(headers)) {
      throw new MessageParseError(this.name, raw.msgId, "its headers are not a JSON object");
    }
    return { ...raw, headers, message: this.#parseBody(raw) };
  }

  #parseBody(raw: RawMessage): T {
    if (this.#parse === undefined) {
      // No parse function: the caller chose `T` and vouched for the queue's contents (see QueueOptions).
      return raw.message as T;
    }
    try {
      return this.#parse(raw.message);
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      throw new MessageParseError(this.name, raw.msgId, reason, { cause });
    }
  }
}
