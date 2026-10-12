import { toPgmqError } from "./errors.js";
import type { Executor } from "./executor.js";
import { Queue, type QueueOptions } from "./queue.js";
import { decodeMetrics, decodeQueueInfos } from "./rows.js";
import * as sql from "./sql.js";
import type { QueueInfo, QueueMetrics } from "./types.js";

/**
 * The entry point: binds an executor and hands out {@link Queue} handles.
 *
 * pgmq-ts never opens connections or manages transactions. You create the Bun SQL client,
 * you decide which transaction an operation runs in (with `.with(tx)`), and you close it.
 *
 * @example
 * import { SQL } from "bun";
 * import { Pgmq } from "pgmq-ts";
 *
 * const sql = new SQL(process.env.DATABASE_URL);
 * const pgmq = new Pgmq(sql);
 * const orders = pgmq.queue("orders");
 */
export class Pgmq {
  readonly #db: Executor;

  /**
   * @param executor Where queries run: a Bun `SQL` client, a transaction, a savepoint, or a
   *   reserved connection.
   */
  constructor(executor: Executor) {
    if (typeof executor !== "function") {
      throw new TypeError("Pgmq needs a Bun SQL client, transaction, or reserved connection, for example new Pgmq(new SQL(url))");
    }
    this.#db = executor;
  }

  /** Returns a `Pgmq` bound to another executor, for example a transaction. */
  with(executor: Executor): Pgmq {
    return new Pgmq(executor);
  }

  /**
   * Returns a handle to the queue `name`. Nothing is sent to the database: call
   * {@link Queue.create} to create the queue if it does not exist yet.
   *
   * @param name 1 to 47 characters of lowercase letters, digits, and underscores.
   * @throws {InvalidQueueNameError} If the name breaks that rule.
   *
   * @example
   * const events = pgmq.queue("events");                               // Queue<unknown>
   * const orders = pgmq.queue("orders", { parse: OrderSchema.parse }); // Queue<Order>, checked on read
   */
  queue<T = unknown>(name: string, options: QueueOptions<T> = {}): Queue<T> {
    return new Queue<T>(this.#db, name, options.parse);
  }

  /** Lists every queue, sorted by name. Wraps `pgmq.list_queues`. */
  async listQueues(): Promise<QueueInfo[]> {
    try {
      return decodeQueueInfos(await sql.listQueues(this.#db));
    } catch (error) {
      throw toPgmqError(error, "pgmq.list_queues");
    }
  }

  /** Returns {@link QueueMetrics} for every queue, sorted by name. Wraps `pgmq.metrics_all`. */
  async metricsAll(): Promise<QueueMetrics[]> {
    try {
      return decodeMetrics(await sql.metricsAll(this.#db));
    } catch (error) {
      throw toPgmqError(error, "pgmq.metrics_all");
    }
  }
}
