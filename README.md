# pgmq-ts

[![CI](https://github.com/uchkunr/pgmq-ts/actions/workflows/ci.yml/badge.svg)](https://github.com/uchkunr/pgmq-ts/actions/workflows/ci.yml)

A type-safe [PGMQ](https://github.com/pgmq/pgmq) client for [Bun](https://bun.com), built on Bun's SQL client, with no runtime dependencies.

Every operation can run inside your own transaction, so a row and the message about it commit or roll back together:

```ts
await sql.begin(async (tx) => {
  const [order] = await tx`INSERT INTO orders (item) VALUES (${"coffee"}) RETURNING id`;
  await orderEvents.with(tx).send({ orderId: order.id });
});
```

![A transaction that throws leaves no order row and no message; the same transaction without the throw commits both](https://raw.githubusercontent.com/uchkunr/pgmq-ts/main/media/pgmq-ts-demo.gif)

Requires Bun 1.2.23+ and Postgres with PGMQ 1.10.0+ (tested up to 1.13.0).

## Quickstart

Start Postgres with PGMQ:

```bash
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 54329:5432 ghcr.io/pgmq/pg18-pgmq:v1.13.0
docker exec pgmq sh -c 'until pg_isready -h 127.0.0.1 -U postgres -q; do sleep 1; done; psql -h 127.0.0.1 -U postgres -c "CREATE EXTENSION IF NOT EXISTS pgmq"'
```

Create a project:

```bash
mkdir hello-pgmq && cd hello-pgmq && bun init -y && bun add pgmq-ts
```

`index.ts`:

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

pgmq-ts never opens connections or manages transactions. It runs each operation on the executor you give it, which can be any Bun SQL handle: the client, a `tx` from `sql.begin`, a savepoint, or `sql.reserve()`. `.with(executor)` rebinds a queue to another one.

Reads are transactional too. Read, do your writes, and delete the message in one transaction, and the work happens once:

```ts
await sql.begin(async (tx) => {
  const [message] = await orderEvents.with(tx).read({ vt: 30 });
  if (!message) return;
  await tx`UPDATE inventory SET reserved = reserved + 1 WHERE item = ${message.message.item}`;
  await orderEvents.with(tx).delete(message.msgId);
});
```

If that transaction rolls back, the read is undone too and the message is visible again immediately.

## Typed messages

```ts
pgmq.queue("events");                                     // Queue<unknown>
pgmq.queue<Order>("orders");                              // typed, not checked
pgmq.queue("orders", { parse: (raw) => Order.parse(raw) }); // typed and checked on read
```

`parse` is any function that returns a `T` or throws, so Zod, Valibot, or your own checks all work. Bodies are stored as `jsonb` (they go through `JSON.stringify`), and message ids are `bigint`s.

## Worker

```ts
import { Worker } from "pgmq-ts";

const worker = new Worker(sql, {
  queue: "payments",
  parse: (raw) => Payment.parse(raw),
  vt: 30,
  concurrency: 4,
  maxAttempts: 5,
  backoff: { baseSeconds: 2, maxSeconds: 300 },
  deadLetterQueue: "payments_dlq",
  handler: async (message) => {
    await chargeCard(message.message);
  },
});

const controller = new AbortController();
process.once("SIGTERM", () => controller.abort());
await worker.run({ signal: controller.signal });
```

When the handler returns, the message is deleted (or archived, with `onSuccess: "archive"`). When it throws, the message is retried later. A message read more than `maxAttempts` times is moved to the dead-letter queue, with the send and the delete in one transaction; without a `deadLetterQueue`, it is archived. Aborting the signal stops reading and waits for running handlers, up to `shutdownTimeoutMs`.

All options and their defaults are documented on `WorkerOptions`.

## Semantics

- **At-least-once.** A read hides a message for `vt` seconds. If it is not deleted or archived in that time, it is delivered again with `readCount` one higher. Handlers must be idempotent.
- **`pop` is at-most-once.** It reads and deletes in one step.
- **Units.** `vt` and `delay` are whole seconds; `delay` can also be a `Date`.
- **Ordering.** Oldest messages first, but concurrency and retries can reorder handling.
- **Queue names.** 1 to 47 characters: lowercase letters, digits, `_`.
- **Errors.** `PgmqError` with the original error as `cause`, plus `QueueNotFoundError`, `InvalidQueueNameError`, and `MessageParseError`. Invalid arguments throw `TypeError` or `RangeError` before any query runs.

## Compared with Amazon SQS

| | pgmq-ts + PGMQ | SQS |
|---|---|---|
| Runs on | your Postgres | AWS |
| Send in your transaction | yes | no (needs an outbox) |
| Delivery | at-least-once | at-least-once |
| Retention | until deleted | up to 14 days |
| Max message | ~255 MB (`jsonb`) | 1 MiB |
| Throughput | your database's | nearly unlimited |

## Development

```bash
scripts/db.sh start          # PGMQ on localhost:54329
bun run typecheck            # strict tsc, the only quality gate
bun examples/worker-dlq.ts   # every example checks its own claims
```

Design decisions and their reasons are in [DECISIONS.md](DECISIONS.md).

## Publishing

1. Bump `version` and update `CHANGELOG.md`.
2. Run `bun run typecheck` and the examples.
3. Run `npm pack --dry-run` and check that it contains only `dist/`, the README, the changelog, and the license.
4. Run `npm publish` (it typechecks and builds first), then `git tag v<version>`.

## Future work

FIFO groups, topic routing, partitioned queues, conditional reads, LISTEN/NOTIFY wake-ups for the worker, and a test suite.

## License

[MIT](LICENSE)
