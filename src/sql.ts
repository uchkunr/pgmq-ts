// Every SQL statement pgmq-ts sends lives in this file, so they can be audited in one place.
//
// Rules that every statement follows:
// - Values are always tagged-template parameters. Nothing is ever concatenated into SQL text.
// - Every parameter has an explicit cast, so Postgres picks exactly one of PGMQ's overloads.
// - JSON and bigint values are bound as text and cast server-side (`::text::jsonb`,
//   `::text::bigint`, `::text::bigint[]`). Bun treats JavaScript strings bound to a `jsonb`
//   parameter as JSON strings, and binds JavaScript arrays as plain text, so this is the only
//   binding that behaves the same everywhere.
// - Every bigint column is selected as `::text` (see rows.ts).
//
// The functions return the executor's raw result; rows.ts decodes it.

import type { Executor } from "./executor.js";

// Queue management

export function create(db: Executor, queue: string): PromiseLike<unknown> {
  return db`SELECT pgmq.create(${queue}::text)`;
}

export function createUnlogged(db: Executor, queue: string): PromiseLike<unknown> {
  return db`SELECT pgmq.create_unlogged(${queue}::text)`;
}

export function dropQueue(db: Executor, queue: string): PromiseLike<unknown> {
  return db`SELECT pgmq.drop_queue(${queue}::text) AS dropped`;
}

export function purgeQueue(db: Executor, queue: string): PromiseLike<unknown> {
  return db`SELECT pgmq.purge_queue(${queue}::text)::text AS purged`;
}

export function listQueues(db: Executor): PromiseLike<unknown> {
  return db`
    SELECT queue_name::text AS queue_name, is_partitioned, is_unlogged, created_at
    FROM pgmq.list_queues()
    ORDER BY queue_name`;
}

export function metrics(db: Executor, queue: string): PromiseLike<unknown> {
  return db`
    SELECT queue_name,
           queue_length::text AS queue_length,
           queue_visible_length::text AS queue_visible_length,
           newest_msg_age_sec,
           oldest_msg_age_sec,
           total_messages::text AS total_messages,
           scrape_time
    FROM pgmq.metrics(${queue}::text)`;
}

export function metricsAll(db: Executor): PromiseLike<unknown> {
  return db`
    SELECT queue_name,
           queue_length::text AS queue_length,
           queue_visible_length::text AS queue_visible_length,
           newest_msg_age_sec,
           oldest_msg_age_sec,
           total_messages::text AS total_messages,
           scrape_time
    FROM pgmq.metrics_all()
    ORDER BY queue_name`;
}

// Sending. `headers` is JSON text or null. `delay` is whole seconds from now or an absolute time.

export function send(
  db: Executor,
  queue: string,
  message: string,
  headers: string | null,
  delay: number | Date,
): PromiseLike<unknown> {
  return typeof delay === "number"
    ? db`
        SELECT msg_id::text AS msg_id
        FROM pgmq.send(${queue}::text, ${message}::text::jsonb, ${headers}::text::jsonb, ${delay}::int)
          AS sent(msg_id)`
    : db`
        SELECT msg_id::text AS msg_id
        FROM pgmq.send(${queue}::text, ${message}::text::jsonb, ${headers}::text::jsonb, ${delay}::timestamptz)
          AS sent(msg_id)`;
}

/**
 * `messages` is a JSON array of the message bodies; `headers` is a JSON array of the same length
 * or null. Both are expanded in order (WITH ORDINALITY), and the ids come back in that order.
 * A JSON `null` header element becomes SQL NULL, the same as a message sent without headers.
 */
export function sendBatch(
  db: Executor,
  queue: string,
  messages: string,
  headers: string | null,
  delay: number | Date,
): PromiseLike<unknown> {
  return typeof delay === "number"
    ? db`
        SELECT msg_id::text AS msg_id
        FROM pgmq.send_batch(
          ${queue}::text,
          (SELECT array_agg(m ORDER BY i)
             FROM jsonb_array_elements(${messages}::text::jsonb) WITH ORDINALITY AS t(m, i)),
          (SELECT array_agg(NULLIF(h, 'null'::jsonb) ORDER BY i)
             FROM jsonb_array_elements(${headers}::text::jsonb) WITH ORDINALITY AS t(h, i)),
          ${delay}::int
        ) WITH ORDINALITY AS sent(msg_id, n)
        ORDER BY n`
    : db`
        SELECT msg_id::text AS msg_id
        FROM pgmq.send_batch(
          ${queue}::text,
          (SELECT array_agg(m ORDER BY i)
             FROM jsonb_array_elements(${messages}::text::jsonb) WITH ORDINALITY AS t(m, i)),
          (SELECT array_agg(NULLIF(h, 'null'::jsonb) ORDER BY i)
             FROM jsonb_array_elements(${headers}::text::jsonb) WITH ORDINALITY AS t(h, i)),
          ${delay}::timestamptz
        ) WITH ORDINALITY AS sent(msg_id, n)
        ORDER BY n`;
}

// Reading. Each returns PGMQ's message_record columns.

export function read(db: Executor, queue: string, vt: number, qty: number): PromiseLike<unknown> {
  return db`
    SELECT msg_id::text AS msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
    FROM pgmq.read(${queue}::text, ${vt}::int, ${qty}::int)`;
}

export function pop(db: Executor, queue: string, qty: number): PromiseLike<unknown> {
  return db`
    SELECT msg_id::text AS msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
    FROM pgmq.pop(${queue}::text, ${qty}::int)`;
}

/** `msgId` is decimal text. `vt` is whole seconds from now or an absolute time. */
export function setVt(db: Executor, queue: string, msgId: string, vt: number | Date): PromiseLike<unknown> {
  return typeof vt === "number"
    ? db`
        SELECT msg_id::text AS msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
        FROM pgmq.set_vt(${queue}::text, ${msgId}::text::bigint, ${vt}::int)`
    : db`
        SELECT msg_id::text AS msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
        FROM pgmq.set_vt(${queue}::text, ${msgId}::text::bigint, ${vt}::timestamptz)`;
}

// Acknowledging. `msgId` is decimal text; `msgIds` is a Postgres array literal such as {1,2,3}.

export function deleteOne(db: Executor, queue: string, msgId: string): PromiseLike<unknown> {
  return db`SELECT pgmq.delete(${queue}::text, ${msgId}::text::bigint) AS deleted`;
}

export function deleteMany(db: Executor, queue: string, msgIds: string): PromiseLike<unknown> {
  return db`
    SELECT msg_id::text AS msg_id
    FROM pgmq.delete(${queue}::text, ${msgIds}::text::bigint[]) AS deleted(msg_id)`;
}

export function archiveOne(db: Executor, queue: string, msgId: string): PromiseLike<unknown> {
  return db`SELECT pgmq.archive(${queue}::text, ${msgId}::text::bigint) AS archived`;
}

export function archiveMany(db: Executor, queue: string, msgIds: string): PromiseLike<unknown> {
  return db`
    SELECT msg_id::text AS msg_id
    FROM pgmq.archive(${queue}::text, ${msgIds}::text::bigint[]) AS archived(msg_id)`;
}
