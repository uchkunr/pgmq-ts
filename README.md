# pgmq-ts

A small, type-safe [PGMQ](https://github.com/pgmq/pgmq) client for [Bun](https://bun.com), built on Bun's own SQL client.

Its defining property: **every operation can run inside your own transaction.** Insert a business row and send the message about it in one transaction, and they commit or roll back together. No outbox table, no relay process, no second datastore.

```ts
await sql.begin(async (tx) => {
  const [order] = await tx`INSERT INTO orders (item) VALUES (${"coffee"}) RETURNING id`;
  await orderEvents.with(tx).send({ orderId: order.id, item: "coffee" });
}); // both committed, or neither
```

- **Thin.** Each method wraps one PGMQ SQL function. No hidden connections, no background tasks, no global state.
- **Typed.** `Queue<T>` types `send` and `read`. An optional `parse` hook (Zod, Valibot, or any function) checks messages on the way out.
- **Exact.** Message ids are `bigint`s, never rounded. Every query is parameterized with explicit casts.
- **Zero runtime dependencies.** Bun only, ESM only.
- **A Worker** with concurrency, retries with exponential backoff, an atomic dead-letter queue, and graceful shutdown.

Requires Bun 1.2.23 or later and Postgres with the PGMQ extension, version 1.10.0 or later (tested up to 1.13.0).

## Contents

- [60-second quickstart](#60-second-quickstart)
- [Transactions](#transactions)
- [Typed messages](#typed-messages)
- [Worker](#worker)
- [API](#api)
- [Semantics](#semantics)
- [Errors](#errors)
- [Compared with Amazon SQS](#compared-with-amazon-sqs)
- [Development](#development)
- [Publishing](#publishing)
- [Future work](#future-work)

## 60-second quickstart

Start Postgres with PGMQ and enable the extension:

```bash
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 54329:5432 ghcr.io/pgmq/pg18-pgmq:v1.13.0
```

```bash
docker exec pgmq sh -c 'until pg_isready -h 127.0.0.1 -U postgres -q; do sleep 1; done; psql -h 127.0.0.1 -U postgres -c "CREATE EXTENSION IF NOT EXISTS pgmq"'
```

Create a project and install:

```bash
mkdir hello-pgmq && cd hello-pgmq && bun init -y && bun add pgmq-ts
```

Put this in `index.ts`:

```ts
import { SQL } from "bun";
import { Pgmq } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:54329/postgres");
const pgmq = new Pgmq(sql);

type Greeting = { to: string; text: string };

// The type argument types send() and read(). Pass { parse } to also check messages on read.
const greetings = pgmq.queue<Greeting>("quickstart_greetings");
await greetings.create(); // no-op if it already exists

const msgId = await greetings.send({ to: "world", text: "hello" });
console.log("sent message", msgId);

// Read one message and hide it from other readers for 30 seconds.
const [message] = await greetings.read({ vt: 30 });
if (message) {
  console.log(`received message ${message.msgId}:`, message.message);
  await greetings.delete(message.msgId); // done with it; otherwise it reappears after 30 s
}

console.log(await greetings.metrics());

await greetings.drop();
await sql.close();
```

```bash
bun index.ts
```

## Transactions

pgmq-ts never opens connections and never begins, commits, or rolls back a transaction. You pass it an **executor** and it runs each operation there. Any Bun SQL handle is an executor:

| Executor | Operations run... |
|---|---|
| `new SQL(url)` | on a pooled connection, each in its own implicit transaction |
| `tx` in `sql.begin(async (tx) => ...)` | inside your transaction |
| `sp` in `tx.savepoint(async (sp) => ...)` | inside your savepoint |
| `await sql.reserve()` | on that one connection |

`new Pgmq(executor)` binds one, and `.with(executor)` rebinds a queue (or the whole `Pgmq`) to another. Handles are immutable and cheap, so `queue.with(tx)` costs nothing.

```ts
const sql = new SQL(process.env.DATABASE_URL);
const pgmq = new Pgmq(sql);
const orderEvents = pgmq.queue<OrderPlaced>("order_events");

await sql.begin(async (tx) => {
  const [order] = await tx`INSERT INTO orders (item) VALUES (${item}) RETURNING id`;
  await orderEvents.with(tx).send({ orderId: order.id, item });
  // Until COMMIT, no consumer can see this message.
});
```

If the transaction rolls back, the message was never sent. If it commits, the message is there, with no window in which the row exists without its message (or the reverse).

Reads are transactional too. A message read inside a transaction that rolls back is visible again at once, and its `readCount` is not incremented. Combined with acknowledging in the same transaction, this gives you **exactly-once effects** for work that lives in the same database:

```ts
await sql.begin(async (tx) => {
  const [message] = await orderEvents.with(tx).read({ vt: 30 });
  if (!message) return;
  await tx`UPDATE inventory SET reserved = reserved + 1 WHERE item = ${message.message.item}`;
  await orderEvents.with(tx).delete(message.msgId);
}); // the update and the delete commit together
```

`examples/transactional-send.ts` demonstrates all three cases: commit, rollback, and a rolled-back read.

## Typed messages

`T` in `Queue<T>` is the type of the message body. There are three ways to get a queue:

```ts
pgmq.queue("events");                       // Queue<unknown>: nothing assumed
pgmq.queue<Order>("orders");                // Queue<Order>: typed, but read() trusts the database
pgmq.queue("orders", { parse: parseOrder }); // Queue<Order>: typed and checked on every read
```

`parse` is any `(raw: unknown) => T` that throws on bad input:

```ts
// Zod
const orders = pgmq.queue("orders", { parse: (raw) => OrderSchema.parse(raw) });

// Valibot
const orders = pgmq.queue("orders", { parse: v.parser(OrderSchema) });

// Any Standard Schema (https://standardschema.dev): a five-line adapter
function standard<T>(schema: StandardSchemaV1<unknown, T>): (raw: unknown) => T {
  return (raw) => {
    const result = schema["~standard"].validate(raw);
    if (result instanceof Promise) throw new TypeError("async schemas are not supported");
    if (result.issues) throw new Error(result.issues.map((issue) => issue.message).join("; "));
    return result.value;
  };
}
```

When `parse` throws, `read` rejects with a `MessageParseError` carrying the queue and `msgId`. The message is not lost: it becomes visible again after its visibility timeout.

Bodies are stored as `jsonb`, so they go through `JSON.stringify`. A `Date` comes back as an ISO string, `undefined` properties disappear, and `bigint` values are rejected. `parse` is the place to turn them back into what you sent.

**Headers** are an optional JSON object stored next to the body: `send(body, { headers: { traceId } })`.

**Message ids** are `bigint`s, because Postgres stores them as `bigint`. Note that `JSON.stringify` cannot serialize a `bigint`. Log `String(message.msgId)` or `message.msgId.toString()` instead.

## Worker

```ts
import { SQL } from "bun";
import { Pgmq, Worker } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL);
await new Pgmq(sql).queue("payments_dlq").create(); // the dead-letter queue must exist

const worker = new Worker(sql, {
  queue: "payments",
  parse: (raw) => PaymentSchema.parse(raw),
  vt: 30,                     // seconds; longer than your slowest handler
  concurrency: 4,             // handlers running at once
  maxAttempts: 5,             // then dead-letter
  backoff: { baseSeconds: 2, maxSeconds: 300 },
  deadLetterQueue: "payments_dlq",
  handler: async (message, { signal }) => {
    await chargeCard(message.message, { signal }); // must be idempotent
  },
});

const controller = new AbortController();
process.once("SIGTERM", () => controller.abort());
await worker.run({ signal: controller.signal }); // resolves after a graceful shutdown
await sql.close();
```

For each message:

1. If `readCount > maxAttempts`, the message is **dead-lettered** without calling the handler. It is sent to `deadLetterQueue` and deleted from the source in **one transaction**, with its body and headers copied server-side exactly as stored. Without a `deadLetterQueue`, it is archived instead.
2. Otherwise the body is parsed and the handler is called.
3. If the handler **returns**, the message is deleted (or archived, with `onSuccess: "archive"`).
4. If the handler or `parse` **throws**, the message is retried. It reappears when its visibility timeout expires, or after `min(maxSeconds, baseSeconds * 2^(attempt - 1))` seconds with `backoff`.

| Option | Default | |
|---|---|---|
| `queue` | required | queue to consume |
| `handler` | required | `(message, { signal }) => Promise<void>` |
| `vt` | required | visibility timeout, whole seconds |
| `parse` | none | checks each body; a failure counts as a failed attempt |
| `concurrency` | 1 | handlers running at once |
| `batchSize` | `concurrency` | most messages per read; never more than free slots |
| `pollIntervalMs` | 1000 | wait when the queue is empty |
| `onSuccess` | `"delete"` | or `"archive"` |
| `maxAttempts` | 5 | `Infinity` retries forever |
| `deadLetterQueue` | none | otherwise exhausted messages are archived |
| `backoff` | none | `{ baseSeconds, maxSeconds }` |
| `shutdownTimeoutMs` | 30000 | how long shutdown waits for handlers |
| `onError` | `console.error` | `(error, { stage, msgId? }) => void` |

**Shutdown.** Aborting the signal stops reading at once and waits for running handlers. If they are still running after `shutdownTimeoutMs`, their `signal` is aborted and `run()` resolves anyway. Their messages are not lost: they are delivered again after the visibility timeout.

**Errors never stop the worker.** A failed read (database down), handler error, failed acknowledgement, and so on all go to `onError` with a `stage` (`read`, `handle`, `ack`, `retry`, `dead-letter`, `shutdown`), and the worker carries on.

**Acknowledging in your own transaction.** A handler can delete the message itself, in the same transaction as its writes (`queue.with(tx).delete(message.msgId)`). The worker's own acknowledgement then finds nothing to do, which is fine.

See `examples/worker-dlq.ts` and `examples/worker-shutdown.ts`.

## API

```ts
new Pgmq(executor)
  .with(executor): Pgmq
  .queue<T>(name, { parse? }): Queue<T>
  .listQueues(): Promise<QueueInfo[]>             // pgmq.list_queues
  .metricsAll(): Promise<QueueMetrics[]>          // pgmq.metrics_all

Queue<T>
  .name: string
  .with(executor): Queue<T>
  .create(): Promise<void>                        // pgmq.create, idempotent
  .createUnlogged(): Promise<void>                // pgmq.create_unlogged
  .drop(): Promise<boolean>                       // pgmq.drop_queue; false if missing
  .purge(): Promise<bigint>                       // pgmq.purge_queue; count deleted
  .metrics(): Promise<QueueMetrics>               // pgmq.metrics
  .send(message, { delay?, headers? }): Promise<bigint>
  .sendBatch(messages, { delay?, headers? }): Promise<bigint[]>
  .read({ vt, qty? }): Promise<Message<T>[]>
  .readWithPoll({ vt, qty?, maxPollSeconds?, pollIntervalMs?, signal? }): Promise<Message<T>[]>
  .pop({ qty? }): Promise<Message<T>[]>
  .setVt(msgId, vt): Promise<Message<T> | null>
  .delete(msgId): Promise<boolean>
  .deleteBatch(msgIds): Promise<bigint[]>
  .archive(msgId): Promise<boolean>
  .archiveBatch(msgIds): Promise<bigint[]>

new Worker<T>(sql, options)
  .run({ signal? }): Promise<void>

Message<T> = { msgId: bigint; readCount: number; enqueuedAt: Date; lastReadAt: Date | null;
               vt: Date; message: T; headers: MessageHeaders | null }
```

Units follow PGMQ: `vt` and `delay` are **whole seconds**, or an absolute `Date` where noted, and `pollIntervalMs` is milliseconds. Invalid arguments throw a `TypeError` or `RangeError` before any query runs.

Queue names are 1 to 47 characters of lowercase letters, digits, and underscores. PGMQ allows more, but it lowercases table names while keeping the original spelling in its catalog, so `Orders` and `orders` would collide.

Every method has JSDoc in the source and in the published `.d.ts` files.

## Semantics

**At-least-once delivery.** `read` does not remove a message. It hides it for `vt` seconds. If the message is not deleted or archived within that time, it is delivered again with `readCount` one higher. A crash, a timeout, or a failed acknowledgement can therefore deliver the same message twice. **Handlers must be idempotent.** Run handler writes and the acknowledgement in one transaction (see [Transactions](#transactions)) when the work lives in the same database.

**Visibility timeout.** Choose `vt` longer than your slowest handler. If a message takes longer than expected, `setVt(msgId, seconds)` extends its lease, and `setVt(msgId, 0)` hands it back to other readers immediately.

**`pop` is at-most-once.** It reads and deletes in one statement, so a message is gone even if your process crashes before handling it. Inside a transaction, a rollback restores it.

**Ordering.** Each read returns the oldest visible messages first (lowest `msg_id`). Concurrent consumers, retries, and delays mean messages can be *handled* out of order. Strict per-key ordering (PGMQ's FIFO groups) is not supported yet.

**Batches** are atomic: `sendBatch` sends every message or none, and returns ids in input order.

**Delays.** `send(message, { delay: 60 })` or `{ delay: someDate }` keeps a message invisible until then.

**Unlogged queues** (`createUnlogged`) are faster but not crash-safe or replicated: a crash or failover empties them. Sends to them are still transactional.

**Polling.** `readWithPoll` polls with `read` from the client, every `pollIntervalMs`, rather than calling PGMQ's `read_with_poll`. Bun cannot cancel a query that is already running, so a server-side poll would ignore the `AbortSignal` and hold a connection for up to `maxPollSeconds`. Client-side, an abort takes effect immediately, and messages a read has already hidden are always returned.

## Errors

| Error | When |
|---|---|
| `PgmqError` | Base class. Any database failure, with the original error (usually Bun's `SQL.PostgresError`) as `cause` and the operation and queue in the message |
| `QueueNotFoundError` | The queue does not exist (SQLSTATE 42P01). Has `.queue` |
| `InvalidQueueNameError` | The name breaks the naming rule. Thrown before any query |
| `MessageParseError` | `parse` threw, or headers are not a JSON object. Has `.queue` and `.msgId` |
| `TypeError`, `RangeError` | Invalid arguments, such as `vt: -1`. Thrown before any query |

```ts
try {
  await queue.send(message);
} catch (error) {
  if (error instanceof QueueNotFoundError) await queue.create();
  else throw error;
}
```

## Compared with Amazon SQS

PGMQ deliberately mirrors SQS's model, so the concepts carry over.

| | pgmq-ts + PGMQ | Amazon SQS |
|---|---|---|
| Infrastructure | your existing Postgres | a separate AWS service |
| Send in the same transaction as your data | **yes**, `queue.with(tx).send(...)` | no, needs an outbox |
| Delivery | at-least-once (`read`), at-most-once (`pop`) | at-least-once (standard queues) |
| Visibility timeout | `vt`, per read, in seconds | per queue or per receive |
| Delay | per message, seconds or a timestamp | per queue or per message, up to 15 min |
| Long polling | `readWithPoll` | `WaitTimeSeconds`, up to 20 s |
| Dead-letter queue | `Worker` with `maxAttempts` + `deadLetterQueue` | redrive policy |
| Message size | Postgres `jsonb`, up to about 255 MB | 1 MiB |
| Retention | until deleted; archive table for history | up to 14 days |
| Throughput | bound by your database | effectively unlimited |
| FIFO | not yet in pgmq-ts | FIFO queues |

Pick SQS for very high throughput or when the producer is not your database. Pick PGMQ when messages must agree with your data.

## Development

```bash
scripts/db.sh start          # Postgres 18 + PGMQ 1.13.0 on localhost:54329 (docker compose)
```

```bash
bun run typecheck            # tsc --noEmit: the quality gate (strict, no linter)
```

```bash
bun examples/worker-dlq.ts   # every file in examples/ runs and checks its own claims
```

```bash
scripts/db.sh stop           # stop the container and delete its data
```

Examples read `DATABASE_URL` and default to `postgres://postgres:postgres@localhost:54329/postgres`. `PGMQ_PORT` and `PGMQ_IMAGE_TAG` change the container's port and PGMQ version. Design decisions, and the sources behind them, are in [DECISIONS.md](DECISIONS.md).

## Publishing

There is no release tooling yet. To publish by hand:

1. Update `version` in `package.json` and add the release to `CHANGELOG.md`.
2. Check: `bun install`, `bun run typecheck`, then `scripts/db.sh start` and run every example.
3. Build: `bun run build` (the bundled `dist/index.js` from `bun build`, plus `.d.ts` files from `tsc`).
4. Inspect the tarball: `npm pack --dry-run`. It should contain only `dist/`, `README.md`, `CHANGELOG.md`, `LICENSE`, and `package.json`.
5. Publish: `npm publish` (runs `prepublishOnly`: typecheck and build). Add `--access public` for a scoped name.
6. Tag the release: `git tag v0.1.0 && git push --tags`.

## Future work

Out of scope for 0.1, and designed to be addable without breaking changes:

- FIFO groups (`read_grouped*` and `x-pgmq-group` headers), as options on `send`, `read`, and the worker
- Topic routing (`send_topic`, `bind_topic`) on `Pgmq`
- Partitioned queues (`create_partitioned`, which needs pg_partman)
- Conditional reads (PGMQ's `conditional` jsonb filter)
- Waking the worker with `LISTEN`/`NOTIFY` (`enable_notify_insert`) instead of polling
- Provenance headers on dead-lettered messages (source queue, original id, last error)
- Jitter for backoff
- `defaultPartitionLength` in metrics (needs PGMQ 1.13)
- Batch `setVt`
- An automated test suite and a CI matrix across PGMQ 1.10 to 1.13 and Bun 1.2.23 to latest

## License

[MIT](LICENSE)
