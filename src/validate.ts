import { InvalidQueueNameError } from "./errors.js";
import type { MessageHeaders } from "./types.js";

/**
 * PGMQ's limit. Its longest generated identifier is `archived_at_idx_<queue>`, and Postgres
 * identifiers are at most 63 characters: 63 - 16 = 47.
 */
const MAX_QUEUE_NAME_LENGTH = 47;

/** Largest value of a Postgres `integer`, the type PGMQ uses for `vt`, `delay` and `qty`. */
const MAX_INT4 = 2_147_483_647;

/** Range of a Postgres `bigint`, the type of `msg_id`. */
const MIN_INT8 = -(2n ** 63n);
const MAX_INT8 = 2n ** 63n - 1n;

/**
 * Checks a queue name against the naming rule and returns it.
 *
 * PGMQ itself accepts more (it only rejects `$`, `;`, `--` and `'`), but it lowercases table
 * names while keeping the original spelling in `pgmq.meta`, so `Orders` and `orders` would
 * share tables yet appear as two queues. Allowing only lowercase rules that out.
 */
export function validateQueueName(name: string): string {
  if (typeof name !== "string") {
    throw new InvalidQueueNameError(String(name), `expected a string, got ${typeof name}`);
  }
  if (name.length === 0) {
    throw new InvalidQueueNameError(name, "it is empty");
  }
  if (name.length > MAX_QUEUE_NAME_LENGTH) {
    throw new InvalidQueueNameError(
      name,
      `it is ${name.length} characters long and the maximum is ${MAX_QUEUE_NAME_LENGTH}`,
    );
  }
  if (!/^[a-z0-9_]+$/.test(name)) {
    throw new InvalidQueueNameError(name, "it contains a character other than a-z, 0-9 or _");
  }
  return name;
}

/** Checks that `value` is an integer that fits a Postgres `integer` and is at least `min`. */
export function validateInt(option: string, value: number, min: number): number {
  if (!Number.isInteger(value) || value < min || value > MAX_INT4) {
    throw new RangeError(`${option} must be an integer from ${min} to ${MAX_INT4}, got ${String(value)}`);
  }
  return value;
}

/** Checks a delay or visibility timeout: whole seconds from now, or an absolute `Date`. */
export function validateSecondsOrDate(option: string, value: number | Date): number | Date {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new RangeError(`${option} is an invalid Date`);
    }
    return value;
  }
  return validateInt(option, value, 0);
}

/** Checks a message id and returns it as decimal text, the form bound to `::text::bigint`. */
export function encodeMsgId(msgId: bigint): string {
  if (typeof msgId !== "bigint") {
    throw new TypeError(`msgId must be a bigint, got ${typeof msgId}. Message ids are bigints, for example 42n or BigInt("42").`);
  }
  if (msgId < MIN_INT8 || msgId > MAX_INT8) {
    throw new RangeError(`msgId ${msgId} is outside the range of a Postgres bigint`);
  }
  return msgId.toString();
}

/** Encodes message ids as a Postgres array literal, bound as text and cast to `::bigint[]`. */
export function encodeMsgIds(msgIds: readonly bigint[]): string {
  if (!Array.isArray(msgIds)) {
    throw new TypeError("msgIds must be an array of bigints");
  }
  return `{${msgIds.map(encodeMsgId).join(",")}}`;
}

/**
 * Serializes a message (or any JSON value) to JSON text, bound as text and cast to `::text::jsonb`.
 *
 * Binding JSON as text matters: Bun JSON-encodes a JavaScript string bound to a `jsonb`
 * parameter, so binding the serialized text directly to `::jsonb` would store a JSON string.
 */
export function encodeJson(value: unknown, what: string): string {
  let json: string | undefined;
  try {
    json = JSON.stringify(value);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new TypeError(`${what} is not JSON-serializable: ${reason}`, { cause });
  }
  if (json === undefined) {
    throw new TypeError(`${what} is not JSON-serializable: JSON.stringify skips values of type ${typeof value}`);
  }
  return json;
}

/** Serializes headers to JSON text. Headers must be a plain JSON object. */
export function encodeHeaders(headers: MessageHeaders, what: string): string {
  if (!isRecord(headers)) {
    throw new TypeError(`${what} must be a plain object, got ${Array.isArray(headers) ? "an array" : typeof headers}`);
  }
  return encodeJson(headers, what);
}

/** True for a non-null, non-array object: the shape of a JSON object. */
export function isRecord(value: unknown): value is MessageHeaders {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
