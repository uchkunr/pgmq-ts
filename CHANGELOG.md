# Changelog

All notable changes to this project are documented here, by hand. Versions follow [Semantic Versioning](https://semver.org).

## 0.1.0 (unreleased)

First release. Requires Bun 1.2.23 or later and PGMQ 1.10.0 or later (tested up to 1.13.0).

### Added

- `Pgmq`: binds a Bun SQL executor (client, transaction, savepoint, or reserved connection) and hands out typed `Queue<T>` handles. `with(executor)` rebinds, so any operation can join the caller's transaction.
- Queue management: `create`, `createUnlogged`, `drop`, `purge`, `metrics`, plus `Pgmq.listQueues` and `Pgmq.metricsAll`.
- Messaging: `send` (delay in seconds or a `Date`, headers), `sendBatch`, `read`, `readWithPoll` (cancellable with an `AbortSignal`), `pop`, `setVt`, `delete`, `deleteBatch`, `archive`, `archiveBatch`.
- Optional `parse` hook per queue, compatible with Zod, Valibot, and Standard Schema adapters.
- Message ids and every Postgres `bigint` as JavaScript `bigint`, whatever the client's `bigint` setting.
- Typed errors: `PgmqError`, `QueueNotFoundError`, `InvalidQueueNameError`, `MessageParseError`.
- `Worker`: concurrency, batch size, visibility timeout, poll interval, delete or archive on success, retries by `readCount` with optional exponential backoff, an atomic dead-letter queue (or archive), and graceful shutdown with a timeout.
- Runnable examples in `examples/`, a Docker Compose setup, and `scripts/db.sh`.
