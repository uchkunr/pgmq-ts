# Decisions

Why pgmq-ts is built the way it is. Each entry gives the decision, the reason, and what was traded away. Everything marked *verified* was checked against PGMQ 1.13.0 on Postgres 18.6 (`ghcr.io/pgmq/pg18-pgmq:v1.13.0`) and Bun 1.4.3, in October 2026.

## Sources

- PGMQ docs, SQL function reference: https://pgmq.github.io/pgmq/ (`docs/api/sql/functions.md`, `types.md`)
- PGMQ repository: https://github.com/pgmq/pgmq. Read: `pgmq-extension/sql/pgmq--*.sql` upgrade scripts 1.3.3 to 1.13.1, `pgmq-rs/src/types/queue_name.rs`, `pgmq-rs/src/queue/`, `pgmq-rs/src/types/mod.rs`
- PGMQ releases: https://github.com/pgmq/pgmq/releases (latest stable: v1.13.0, 2026-09-07)
- Bun SQL docs: https://bun.com/docs/runtime/sql, and the type definitions in `bun-types/sql.d.ts` (1.4.3)
- Context7 (HTTP API, since the MCP server was not connected): libraries `/pgmq/pgmq` and `/oven-sh/bun`, used to cross-check the above
- The running extension itself: `\df pgmq.*`, `pg_get_functiondef` for every function, `\d` for `pgmq.message_record`, `pgmq.queue_record`, and `pgmq.metrics_result`
- Behavior probes run against the container (scratch scripts, not in the repo), whose results are recorded below

When a doc and the running extension disagreed, the extension won. No disagreement affected the design.

## Package name

`pgmq-ts` is available. It was published once (0.1.1) and unpublished on 2024-10-10, so the name can be claimed again. Version 0.1.1 can never be reused, which does not matter because we start at 0.1.0. If the name is ever unavailable, publish as `@<your-scope>/pgmq-ts`. The nearest existing packages are `@baz-scm/pgmq-ts` (a pgmq-rs port, not Bun-specific) and `pgmq-js`.

## PGMQ versions

**Target 1.13.0; minimum 1.10.0.** *Verified* by running every statement in `src/sql.ts` against 1.7.0, 1.10.0, and 1.13.0:

| Feature used | Since |
|---|---|
| `headers` column, `send`/`send_batch` with headers and a timestamptz delay | 1.5.0 |
| `pop(queue, qty)` | 1.7.0 |
| `last_read_at` column | 1.8.0 (1.7.0 fails: column does not exist) |
| `set_vt(queue, bigint, timestamptz)` and batch `set_vt` | 1.10.0 |
| `metrics.default_partition_length` | 1.13.0, so **left out** of `QueueMetrics` |

PGMQ functions wrapped, with the exact overloads called (from `\df pgmq.*`):

| Method | SQL |
|---|---|
| `create` / `createUnlogged` | `pgmq.create(text)` / `pgmq.create_unlogged(text)` → void |
| `drop` | `pgmq.drop_queue(text)` → boolean (the 2-argument form is deprecated) |
| `purge` | `pgmq.purge_queue(text)` → bigint |
| `listQueues` | `pgmq.list_queues()` → `queue_record` |
| `metrics` / `metricsAll` | `pgmq.metrics(text)` / `pgmq.metrics_all()` → `metrics_result` |
| `send` | `pgmq.send(text, jsonb, jsonb, integer \| timestamptz)` → SETOF bigint |
| `sendBatch` | `pgmq.send_batch(text, jsonb[], jsonb[], integer \| timestamptz)` → SETOF bigint |
| `read` | `pgmq.read(text, integer, integer)` (default `conditional`) → SETOF `message_record` |
| `pop` | `pgmq.pop(text, integer)` → SETOF `message_record` |
| `setVt` | `pgmq.set_vt(text, bigint, integer \| timestamptz)` → SETOF `message_record` |
| `delete` / `deleteBatch` | `pgmq.delete(text, bigint)` → boolean / `(text, bigint[])` → SETOF bigint |
| `archive` / `archiveBatch` | `pgmq.archive(text, bigint)` → boolean / `(text, bigint[])` → SETOF bigint |

`send` always uses the 4-argument overloads (headers may be NULL, delay may be 0), so there are two statements, not six. `read_with_poll` is not called (see [Polling](#polling)).

## Bun versions

**`engines.bun: >=1.2.23`.** *Verified*: every example and edge-case probe passes on 1.2.23, 1.3.0, and 1.4.3. Bun 1.2.0 fails `examples/worker-shutdown.ts`. A minimal reproduction without pgmq-ts showed that 1.2.0 **loses concurrent parameterized queries** on a pool (two concurrent `pgmq.delete` calls: one silently did nothing, in 11 of 20 rounds). That is a Bun bug, fixed by 1.2.23. Versions between 1.2.0 and 1.2.23 were not tested.

## How Bun returns bigint, and `msgId`

*Verified*: Bun returns Postgres `int8` as a **string** by default (`"9007199254740993"`), and as a JavaScript `BigInt` when the client is created with `new SQL({ bigint: true })`. `int4` is always a `number`.

**Every `bigint` column is selected as `::text` and converted with `BigInt()` in `src/rows.ts`.** The result is identical whatever the caller's `bigint` option, and never loses precision. `msgId` is a `bigint`. So are all other `int8` values (metrics counts, `purge` results), so there is one rule to remember. Cost: `JSON.stringify` cannot serialize a `bigint` (documented in the README).

Message ids passed in are checked (`typeof === "bigint"`, int8 range) and bound as decimal text: `${id}::text::bigint`.

## Binding JSON: always as text

*Verified*: Bun JSON-encodes a JavaScript **string** bound to a `jsonb`-typed parameter. `${JSON.stringify({a: 1})}::jsonb` stored the JSON *string* `"{\"a\":1}"`, and `${"{bad"}::jsonb` was accepted as the string `"{bad"`. Binding a JavaScript object directly does work, but then a top-level string message would be ambiguous.

**pgmq-ts serializes with `JSON.stringify` itself and binds the text: `${json}::text::jsonb`.** Postgres parses it, so invalid JSON is impossible and every JSON type round-trips (*verified* for objects, arrays, strings that look like JSON, numbers, booleans, and null). `undefined`, functions, and `bigint`s are rejected with a `TypeError` before any query.

## Binding arrays

*Verified*: Bun does not bind a JavaScript array as a Postgres array (`${[5n, 6n]}::bigint[]` fails with "malformed array literal"). `sql.array()` works, but it would make the executor type bigger than a tagged-template function, and its docs warn about NULL elements.

- **Id arrays** are bound as a Postgres array literal, `${"{1,2,3}"}::text::bigint[]`. The elements are validated bigints, so the literal contains only digits and commas.
- **Batches of messages and headers** are bound as one JSON array text and expanded in SQL: `(SELECT array_agg(m ORDER BY i) FROM jsonb_array_elements(${json}::text::jsonb) WITH ORDINALITY AS t(m, i))`. Order is explicit. Headers use `NULLIF(h, 'null'::jsonb)`, so a `null` entry is stored as SQL NULL, like a single send without headers (*verified*). When there are no headers, `array_agg` over zero rows gives NULL, which PGMQ reads as "no headers".
- `send_batch` results are ordered with `WITH ORDINALITY ... ORDER BY`, so ids come back in input order.

## Explicit casts everywhere

Every parameter carries a cast (`::text`, `::int`, `::text::bigint`, `::text::jsonb`, `::timestamptz`), so Postgres resolves PGMQ's overloads (`send` has six) to exactly one function. *Verified*: an uncast `send($1, $2, $3)` happened to resolve too, but nothing should depend on that.

## Queue names

**1 to 47 characters, `[a-z0-9_]` only**, checked before any query (`InvalidQueueNameError`).

- 47 is PGMQ's own limit (`validate_queue_name`): its longest identifier is `archived_at_idx_<queue>`, and Postgres identifiers are at most 63 characters.
- PGMQ itself only rejects `$`, `;`, `--`, and `'`. *Verified*: it accepted `a-b` and `a b`.
- PGMQ lowercases table names (`format_table_name`) but keeps the original spelling in `pgmq.meta`. `MyQueue` and `myqueue` share tables but are two catalog entries, and this caused a real PGMQ bug: insert notifications never fired for mixed-case queues until the unreleased 1.13.1.
- pgmq-rs allows `[A-Za-z0-9_]{1,47}`. We chose **lowercase only** (decided with the maintainer). It is stricter, so it can be relaxed later without a breaking change. The cost: queues created elsewhere with uppercase names cannot be used through pgmq-ts.

## Executor model

`Executor` is a structural type: a tagged-template function returning `PromiseLike<unknown>`. Bun's `SQL`, `TransactionSQL`, `SavepointSQL`, and `ReservedSQL` all satisfy it. This was *verified* at runtime for all four, and by `tsc` in a consumer project built from the packed tarball, where `SQL` and `ReservedSQL` also satisfy `TransactionalExecutor`. Test fakes can be plain functions.

- The library never opens a connection, never calls `begin`/`commit`/`rollback` (except the Worker, below), and never closes the client.
- Handles (`Pgmq`, `Queue`) are immutable. `with(executor)` returns a copy. There is no global state.
- `TransactionalExecutor` adds `begin(fn: (tx) => Promise<void>)`. It is typed with a `void` callback because Bun's `begin<const T>` returns `Promise<ContextCallbackResult<T>>`, which TypeScript will not match against a generic `begin<R>`.

## Transactions: what is and is not atomic

*Verified*: a send inside a rolled-back `sql.begin` leaves no row. A message sent in an open transaction is invisible to readers on other connections until commit. A `read` in a rolled-back transaction is undone: the message is visible at once, and `read_ct` is unchanged. (PGMQ's `read` uses `clock_timestamp()`, so `vt` is wall-clock based even inside a long transaction.)

## Polling

**`readWithPoll` is a client-side loop of `pgmq.read` with an abortable sleep, not a call to `pgmq.read_with_poll`.**

*Verified*: Bun 1.4.3's `query.cancel()` does not stop a running query. It sets `cancelled = true`, but `pg_sleep(3)` still took 3 s, and `read_with_poll` ran its full 10 s. With the server-side function, an `AbortSignal` could only take effect after up to `maxPollSeconds`, while holding a pool connection. Cancelling from a second connection (`pg_cancel_backend`) would break the no-hidden-connections rule.

Client-side, abort is immediate (*verified*: about 2 ms), and a read that already hid messages always returns them. The cost is one round trip per `pollIntervalMs` while the queue is empty. Defaults match PGMQ's: `maxPollSeconds: 5`, `pollIntervalMs: 100`. `maxPollSeconds: 0` does one read, whereas PGMQ's returns without reading. `pollIntervalMs` must be at least 1, to rule out a busy loop.

`sleep.ts` uses `setTimeout`/`clearTimeout`, not `Bun.sleep`, because `Bun.sleep` cannot be cancelled and its timer would keep the process alive after shutdown.

## Units and argument checks

Units follow PGMQ, so its docs read the same: `vt` and `delay` are whole seconds (or an absolute `Date` where PGMQ has a timestamptz overload), and `pollIntervalMs` is milliseconds. Integers are checked against the Postgres `integer` range. Invalid arguments are programming errors, so they throw a plain `TypeError` or `RangeError` before any query. `PgmqError` is reserved for conditions about queues and the database.

## Typed messages and `parse`

- `queue(name)` gives `Queue<unknown>`. `queue<T>(name)` gives a `Queue<T>` that trusts the database. `queue(name, { parse })` infers `T` and checks every message read. The single `as T` in the codebase is in `parse.ts`, for the explicit "trust" case, and is commented.
- `parse` is a plain synchronous `(raw: unknown) => T`, so Zod, Valibot, and hand-written guards plug in directly. Standard Schema needs a five-line adapter (in the README). No validation library is a dependency.
- When `parse` throws, or headers are not a JSON object, the read rejects with `MessageParseError` (queue, msgId, cause). The rest of that batch reappears after its visibility timeout: nothing is lost, but it is delayed. The Worker avoids this by parsing per message.
- `pop` deletes before parsing, so a parse failure there loses the batch unless `pop` runs in a transaction. This is documented in its JSDoc.

## Errors

`toPgmqError` in `src/errors.ts` is the one place database errors are mapped. *Verified*: Bun's `PostgresError` puts the **SQLSTATE in `errno`**, and `code` is always a Bun string like `ERR_POSTGRES_SERVER_ERROR`. 42P01 (`relation "pgmq.q_x" does not exist`) on an operation targeting a queue becomes `QueueNotFoundError`. Everything else becomes `PgmqError` with the operation and queue in the message and the original error as `cause`. `drop()` on a missing queue returns `false`, as PGMQ does (it raises only a NOTICE).

## Small behaviors

- Empty batches (`sendBatch([])`, `deleteBatch([])`, `archiveBatch([])`) return `[]` without a query. PGMQ raises on an empty `send_batch`.
- `listQueues` and `metricsAll` are sorted by queue name. PGMQ returns them in catalog order.
- `create` is idempotent (PGMQ uses `CREATE TABLE IF NOT EXISTS`).
- The library never runs `CREATE EXTENSION pgmq`: it needs elevated privileges, and it is a deployment decision.

## Worker

- **Built only on the queue primitives** (`read`, `set_vt`, `delete`, `archive`, `send`), through the same `sql.ts` statements as `Queue`. It works on raw rows and parses each message itself, so one malformed message (bad body *or* bad headers) fails only itself and still reaches the dead-letter queue. Reading through `Queue.read` would make every batch read throw, and the message would loop forever.
- **Retries** are driven by `readCount`. On a handler failure the message keeps the visibility timeout set by its read. With `backoff`, `setVt` sets `min(maxSeconds, baseSeconds * 2^(readCount-1))` seconds. No jitter, to keep it predictable (future work).
- **Dead-lettering** happens when a message is read with `readCount > maxAttempts`, without calling the handler. That also covers a worker that crashed mid-handler on its last attempt. Defaults (decided with the maintainer): `maxAttempts: 5`, `onSuccess: "delete"`, no backoff. Without a `deadLetterQueue`, exhausted messages are archived, so they never loop forever and are never silently deleted.
- **The dead-letter move is one transaction**: `set_vt(...) CROSS JOIN LATERAL pgmq.send(target, m.message, m.headers, 0)` then `pgmq.delete`. The copy happens server-side, so body and headers arrive exactly as stored (*verified* with `12345678901234567891`, which a JavaScript double would round). `set_vt` is used because it is PGMQ's only function that returns one message by id, and it renews the lease during the move. If the copy finds no message, nothing is deleted. A missing DLQ is reported as `QueueNotFoundError` for the DLQ, and the message stays in the source.
- **Flow control**: the worker never reads more messages than it has free handler slots (`batchSize <= concurrency`, enforced), so no message sits in memory while its visibility timeout runs. Waiting for a slot uses a resolver that is replaced on each wait, not `Promise.race` against a long-lived promise, which would leak handlers in a worker that runs for weeks.
- **Shutdown**: `run({ signal })` was chosen over `start()`/`stop()`, since an AbortSignal is the platform's cancellation primitive. On abort it stops reading, waits up to `shutdownTimeoutMs` (default 30 s), then aborts the handlers' `signal`, reports a `shutdown` error, and resolves. Messages read during the abort are still handled.
- **Errors never stop the worker** and never reject `run`. They go to `onError` with a stage. The default logs with `console.error`, because silently swallowing errors is worse than a log line. An `onError` that throws is caught and logged.
- `vt` is required and at least 1. A worker with `vt: 0` would hand every message to every other consumer at once.

## Non-goals for 0.1

Partitioned queues need pg_partman setup, maintenance, and retention docs. They are not small, so they are a non-goal; every other operation works on partitioned queues created with SQL. Also out of scope: conditional reads, FIFO groups, topic routing, a CLI, and other runtimes. These are listed under Future work in the README. The API leaves room for them: everything takes an options object, and `Pgmq` is the natural home for cross-queue topic operations.

## Tooling

- **TypeScript 7.0.2** (the native compiler) and `@types/bun` are the only dev dependencies. `typescript` was a `peerDependency` in the `bun init` template; it moved to `devDependencies`, because a runtime library does not need consumers to have TypeScript.
- tsconfig: `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, plus `isolatedDeclarations` (every export has an explicit type, so the public API never depends on inference), `noImplicitReturns`, and `noUnused*`. `noPropertyAccessFromIndexSignature` was dropped: it rejects the idiomatic `process.env.DATABASE_URL`, which README users copy.
- Relative imports use `.js` specifiers, so the emitted `.d.ts` files resolve under both `bundler` and `nodenext` module resolution (*verified* in a consumer project that installed the packed tarball).
- Build: `bun build --target bun --format esm` bundles `dist/index.js`, and `tsc -p tsconfig.build.json` emits only `.d.ts` files. Not minified, so stack traces stay readable.
- Examples import `"pgmq-ts"` through a tsconfig `paths` entry (which Bun honors at runtime), so they read exactly like user code.
- The dev database runs on port **54329**, not 5432, so it does not collide with a local Postgres. Examples read `DATABASE_URL` and default to that port.
- `.gitattributes` forces LF, so `scripts/db.sh` runs on Windows checkouts with `core.autocrlf=true`.
