import { MessageParseError } from "./errors.js";
import type { RawMessage } from "./rows.js";
import type { Message } from "./types.js";
import { isRecord } from "./validate.js";

/**
 * Turns a decoded row into a `Message<T>`: checks that the headers are a JSON object and runs
 * the queue's `parse` function on the body. Shared by Queue and Worker so both treat a malformed
 * message the same way.
 *
 * @throws {MessageParseError} If the headers are not an object or `parse` throws.
 */
export function toMessage<T>(queue: string, raw: RawMessage, parse: ((raw: unknown) => T) | undefined): Message<T> {
  const { headers } = raw;
  if (headers !== null && !isRecord(headers)) {
    throw new MessageParseError(queue, raw.msgId, "its headers are not a JSON object");
  }
  return { ...raw, headers, message: parseBody(queue, raw, parse) };
}

function parseBody<T>(queue: string, raw: RawMessage, parse: ((raw: unknown) => T) | undefined): T {
  if (parse === undefined) {
    // No parse function: the caller chose `T` and vouched for the queue's contents (see QueueOptions).
    return raw.message as T;
  }
  try {
    return parse(raw.message);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new MessageParseError(queue, raw.msgId, reason, { cause });
  }
}
