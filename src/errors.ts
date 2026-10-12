/**
 * Base class for every error pgmq-ts raises about a queue or the database. The original error,
 * if any (usually Bun's `SQL.PostgresError`), is attached as `cause`.
 *
 * Invalid arguments, such as a negative `vt`, are programming errors and throw a plain
 * `TypeError` or `RangeError` instead, before any query runs.
 */
export class PgmqError extends Error {
  override name: string = "PgmqError";
}

/** The queue does not exist: PGMQ has no table for it. */
export class QueueNotFoundError extends PgmqError {
  override name: string = "QueueNotFoundError";
  /** The name of the missing queue. */
  readonly queue: string;

  constructor(queue: string, options?: ErrorOptions) {
    super(`Queue "${queue}" does not exist. Create it first with pgmq.queue("${queue}").create().`, options);
    this.queue = queue;
  }
}

/**
 * The queue name breaks the naming rule: 1 to 47 characters, each a lowercase ASCII letter,
 * a digit, or an underscore. Thrown before any query runs.
 */
export class InvalidQueueNameError extends PgmqError {
  override name: string = "InvalidQueueNameError";
  /** The rejected name. */
  readonly queue: string;

  constructor(queue: string, reason: string) {
    super(
      `Invalid queue name "${queue}": ${reason}. ` +
        "Queue names are 1 to 47 characters of lowercase letters, digits, and underscores.",
    );
    this.queue = queue;
  }
}

/**
 * A message read from the queue could not be turned into the queue's message type: the queue's
 * `parse` function threw (the thrown error is the `cause`), or its headers were not a JSON object.
 *
 * The message is not lost. It stays invisible until its visibility timeout expires, then can be
 * read again; use `msgId` to inspect, archive, or delete it.
 */
export class MessageParseError extends PgmqError {
  override name: string = "MessageParseError";
  /** The queue the message was read from. */
  readonly queue: string;
  /** The id of the message that failed to parse. */
  readonly msgId: bigint;

  constructor(queue: string, msgId: bigint, reason: string, options?: ErrorOptions) {
    super(`Message ${msgId} in queue "${queue}" could not be parsed: ${reason}`, options);
    this.queue = queue;
    this.msgId = msgId;
  }
}

/** SQLSTATE 42P01: PGMQ raises it when the queue's table does not exist. */
const UNDEFINED_TABLE = "42P01";

/**
 * The one place errors from the database become pgmq-ts errors.
 *
 * @param operation The PGMQ function that failed, for example `"pgmq.send"`.
 * @param queue The queue the operation targeted, if it targets one.
 */
export function toPgmqError(error: unknown, operation: string, queue?: string): PgmqError {
  if (error instanceof PgmqError) {
    return error;
  }
  if (queue !== undefined && sqlStateOf(error) === UNDEFINED_TABLE) {
    return new QueueNotFoundError(queue, { cause: error });
  }
  const target = queue === undefined ? operation : `${operation} on queue "${queue}"`;
  const detail = error instanceof Error ? error.message : String(error);
  return new PgmqError(`${target} failed: ${detail}`, { cause: error });
}

/**
 * Bun's `SQL.PostgresError` carries the SQLSTATE in `errno`. Its `code` is a Bun-specific
 * string such as `ERR_POSTGRES_SERVER_ERROR`, so it cannot be used here.
 */
function sqlStateOf(error: unknown): string | undefined {
  if (typeof error === "object" && error !== null && "errno" in error && typeof error.errno === "string") {
    return error.errno;
  }
  return undefined;
}
