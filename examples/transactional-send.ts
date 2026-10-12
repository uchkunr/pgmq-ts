// The reason this library exists: queue operations run inside your own transaction.
// A business row and the message about it commit together or roll back together.
//
//   bun examples/transactional-send.ts

import assert from "node:assert/strict";
import { SQL } from "bun";
import { Pgmq } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:54329/postgres");
const pgmq = new Pgmq(sql);

type OrderPlaced = { orderId: string; item: string };
const events = pgmq.queue<OrderPlaced>("example_order_events");

await sql`CREATE TABLE IF NOT EXISTS example_orders (id bigserial PRIMARY KEY, item text NOT NULL)`;
await sql`TRUNCATE example_orders`;
await events.drop();
await events.create();

async function countOrders(): Promise<number> {
  const [row]: { n: number }[] = await sql`SELECT count(*)::int AS n FROM example_orders`;
  return row?.n ?? 0;
}

// 1. Commit: the order row and its event appear together.
await sql.begin(async (tx) => {
  const [order]: { id: string }[] = await tx`INSERT INTO example_orders (item) VALUES (${"coffee"}) RETURNING id`;
  assert(order);
  await events.with(tx).send({ orderId: order.id, item: "coffee" });

  // Until COMMIT, nobody else can see the message. This read runs on the pool, outside the transaction.
  assert.deepEqual(await events.read({ vt: 0 }), []);
  console.log("1. inside the transaction, other readers see no message yet");
});
assert.equal(await countOrders(), 1);
assert.equal((await events.metrics()).queueLength, 1n);
console.log("1. after COMMIT: 1 order row and 1 message");

// 2. Rollback: an error after the send undoes the row and the message.
try {
  await sql.begin(async (tx) => {
    const [order]: { id: string }[] = await tx`INSERT INTO example_orders (item) VALUES (${"tea"}) RETURNING id`;
    assert(order);
    await events.with(tx).send({ orderId: order.id, item: "tea" });
    throw new Error("payment declined");
  });
} catch (error) {
  console.log(`2. transaction failed (${error instanceof Error ? error.message : error}) and rolled back`);
}
assert.equal(await countOrders(), 1);
assert.equal((await events.metrics()).queueLength, 1n);
console.log("2. after ROLLBACK: still 1 order row and 1 message; the tea order and its event never existed");

// 3. Reads are transactional too. A read inside a rolled-back transaction is undone:
//    the message is visible again at once and its readCount is not incremented.
try {
  await sql.begin(async (tx) => {
    const [message] = await events.with(tx).read({ vt: 60 });
    assert.equal(message?.readCount, 1);
    throw new Error("handler crashed");
  });
} catch {
  // expected
}
const [message] = await events.read({ vt: 60 });
assert.equal(message?.readCount, 1);
console.log("3. a read inside a rolled-back transaction was undone: readCount is still 1, no 60 s wait");

await events.drop();
await sql`DROP TABLE example_orders`;
await sql.close();
console.log("ok");
