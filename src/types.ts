/** Message headers: a JSON object stored in PGMQ's `headers` column. */
export type MessageHeaders = { [key: string]: unknown };

/** A message read from a queue, by `read`, `readWithPoll`, `pop` or `setVt`. */
export interface Message<T> {
  /** The message id, unique within its queue. A bigint, because Postgres stores it as `bigint`. */
  readonly msgId: bigint;
  /**
   * How many times the message has been read. A `read` counts itself, so the first read returns 1.
   * `pop` and `setVt` do not count as reads and return the current count, which can be 0.
   */
  readonly readCount: number;
  /** When the message was sent. */
  readonly enqueuedAt: Date;
  /** When the message was last read, or `null` if it never was (possible for `pop` and `setVt`). */
  readonly lastReadAt: Date | null;
  /** When the message becomes visible to readers again. */
  readonly vt: Date;
  /** The message body. */
  readonly message: T;
  /** The message headers, or `null` if it was sent without headers. */
  readonly headers: MessageHeaders | null;
}

/** A queue, as listed by `pgmq.listQueues()`. */
export interface QueueInfo {
  readonly queueName: string;
  /** True for queues created with PGMQ's `create_partitioned` (pg_partman). */
  readonly isPartitioned: boolean;
  /** True for queues created with `createUnlogged`. */
  readonly isUnlogged: boolean;
  readonly createdAt: Date;
}

/** A point-in-time snapshot of a queue, from `queue.metrics()` or `pgmq.metricsAll()`. */
export interface QueueMetrics {
  readonly queueName: string;
  /** Messages in the queue, visible or not. */
  readonly queueLength: bigint;
  /** Messages that can be read right now (their `vt` has passed). */
  readonly queueVisibleLength: bigint;
  /** Age in seconds of the newest message, or `null` if the queue is empty. */
  readonly newestMsgAgeSec: number | null;
  /** Age in seconds of the oldest message, or `null` if the queue is empty. */
  readonly oldestMsgAgeSec: number | null;
  /** Messages ever sent to the queue (the last value of its `msg_id` sequence). */
  readonly totalMessages: bigint;
  /** When the snapshot was taken, on the database clock. */
  readonly scrapeTime: Date;
}
