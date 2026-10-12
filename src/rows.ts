// The one place where rows returned by PGMQ become TypeScript values: snake_case columns map to
// camelCase fields here and nowhere else. Every column is checked at runtime; a value of an
// unexpected type is a bug (in this library, PGMQ, or the driver), never something to cast away.
//
// Every `bigint` column is selected as `::text` and converted with BigInt(), so the result does
// not depend on whether the caller created Bun's client with `bigint: true`.

import { PgmqError } from "./errors.js";
import type { QueueInfo, QueueMetrics } from "./types.js";

type Row = { readonly [column: string]: unknown };

/** A message row before its body is parsed and its headers are checked. */
export interface RawMessage {
  readonly msgId: bigint;
  readonly readCount: number;
  readonly enqueuedAt: Date;
  readonly lastReadAt: Date | null;
  readonly vt: Date;
  readonly message: unknown;
  readonly headers: unknown;
}

export function decodeMessages(result: unknown): RawMessage[] {
  return rowsOf(result).map((row) => ({
    msgId: bigintColumn(row, "msg_id"),
    readCount: intColumn(row, "read_ct"),
    enqueuedAt: dateColumn(row, "enqueued_at"),
    lastReadAt: nullable(row, "last_read_at", dateColumn),
    vt: dateColumn(row, "vt"),
    message: row["message"] ?? null,
    headers: row["headers"] ?? null,
  }));
}

/** Decodes the `msg_id` column of every row: the ids returned by send, delete and archive. */
export function decodeMsgIds(result: unknown): bigint[] {
  return rowsOf(result).map((row) => bigintColumn(row, "msg_id"));
}

export function decodeQueueInfos(result: unknown): QueueInfo[] {
  return rowsOf(result).map((row) => ({
    queueName: textColumn(row, "queue_name"),
    isPartitioned: booleanColumn(row, "is_partitioned"),
    isUnlogged: booleanColumn(row, "is_unlogged"),
    createdAt: dateColumn(row, "created_at"),
  }));
}

export function decodeMetrics(result: unknown): QueueMetrics[] {
  return rowsOf(result).map((row) => ({
    queueName: textColumn(row, "queue_name"),
    queueLength: bigintColumn(row, "queue_length"),
    queueVisibleLength: bigintColumn(row, "queue_visible_length"),
    newestMsgAgeSec: nullable(row, "newest_msg_age_sec", intColumn),
    oldestMsgAgeSec: nullable(row, "oldest_msg_age_sec", intColumn),
    totalMessages: bigintColumn(row, "total_messages"),
    scrapeTime: dateColumn(row, "scrape_time"),
  }));
}

/** Decodes a single-row, single-value result such as `SELECT pgmq.delete(...) AS deleted`. */
export function decodeBoolean(result: unknown, column: string): boolean {
  return booleanColumn(onlyRow(result), column);
}

/** Decodes a single-row `bigint` result such as `SELECT pgmq.purge_queue(...)::text AS purged`. */
export function decodeBigint(result: unknown, column: string): bigint {
  return bigintColumn(onlyRow(result), column);
}

function rowsOf(result: unknown): Row[] {
  if (!Array.isArray(result)) {
    throw unexpected(`query result to be an array of rows, got ${describe(result)}`);
  }
  return result.map((row: unknown) => {
    if (!isRow(row)) {
      throw unexpected(`each row to be an object, got ${describe(row)}`);
    }
    return row;
  });
}

/** Any object can be read column by column; each column is then checked on its own. */
function isRow(value: unknown): value is Row {
  return typeof value === "object" && value !== null;
}

function onlyRow(result: unknown): Row {
  const rows = rowsOf(result);
  const [row] = rows;
  if (row === undefined || rows.length !== 1) {
    throw unexpected(`exactly one row, got ${rows.length}`);
  }
  return row;
}

function textColumn(row: Row, column: string): string {
  const value = row[column];
  if (typeof value !== "string") {
    throw unexpected(`column ${column} to be a string, got ${describe(value)}`);
  }
  return value;
}

function bigintColumn(row: Row, column: string): bigint {
  const text = textColumn(row, column);
  if (!/^-?\d+$/.test(text)) {
    throw unexpected(`column ${column} to hold an integer, got "${text}"`);
  }
  return BigInt(text);
}

function intColumn(row: Row, column: string): number {
  const value = row[column];
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw unexpected(`column ${column} to be an integer, got ${describe(value)}`);
  }
  return value;
}

function booleanColumn(row: Row, column: string): boolean {
  const value = row[column];
  if (typeof value !== "boolean") {
    throw unexpected(`column ${column} to be a boolean, got ${describe(value)}`);
  }
  return value;
}

function dateColumn(row: Row, column: string): Date {
  const value = row[column];
  if (!(value instanceof Date)) {
    throw unexpected(`column ${column} to be a Date, got ${describe(value)}`);
  }
  return value;
}

function nullable<T>(row: Row, column: string, decode: (row: Row, column: string) => T): T | null {
  return row[column] === null || row[column] === undefined ? null : decode(row, column);
}

function unexpected(expectation: string): PgmqError {
  return new PgmqError(`Unexpected result from PGMQ: expected ${expectation}`);
}

function describe(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  return typeof value;
}
