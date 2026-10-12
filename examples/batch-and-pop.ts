// Batch sends with per-message headers, batch reads, setVt, batch delete/archive, and pop.
//
//   bun examples/batch-and-pop.ts

import assert from "node:assert/strict";
import { SQL } from "bun";
import { Pgmq } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:54329/postgres");
const pgmq = new Pgmq(sql);

const tasks = pgmq.queue<{ task: string }>("example_batch_and_pop");
await tasks.drop();
await tasks.create();

// One statement, all or nothing. Ids come back in the order of the messages.
const ids = await tasks.sendBatch([{ task: "a" }, { task: "b" }, { task: "c" }, { task: "d" }], {
  headers: [{ priority: "high" }, null, null, { priority: "low" }],
});
assert.equal(ids.length, 4);
assert(ids.every((id, i) => i === 0 || id > ids[i - 1]!), "ids are in send order");
console.log("sendBatch: sent 4 messages, ids", ids.join(", "));

const batch = await tasks.read({ vt: 30, qty: 10 });
assert.deepEqual(
  batch.map((m) => [m.message.task, m.headers]),
  [["a", { priority: "high" }], ["b", null], ["c", null], ["d", { priority: "low" }]],
);
console.log("read qty 10: got all 4, each with its own headers");

// setVt(id, 0) hands a message back to other readers immediately...
const [, b] = batch;
assert(b);
await tasks.setVt(b.msgId, 0);
const [again] = await tasks.read({ vt: 30 });
assert.equal(again?.msgId, b.msgId);
assert.equal(again.readCount, 2);
console.log(`setVt(${b.msgId}, 0): released at once and read again (readCount 2)`);

// ...and setVt(id, seconds) extends a lease that is about to run out.
const extended = await tasks.setVt(b.msgId, 120);
assert(extended && extended.vt.getTime() - Date.now() > 110_000);
assert.equal(await tasks.setVt(999_999n, 10), null);
console.log("setVt(id, 120): lease extended to", extended.vt.toISOString(), "(unknown ids return null)");

// Batch delete and archive return the ids they found; unknown ids are left out.
const [a, , c, d] = ids;
assert(a && c && d);
assert.deepEqual(await tasks.deleteBatch([a, b.msgId, 999_999n]), [a, b.msgId]);
assert.deepEqual(await tasks.archiveBatch([c]), [c]);
assert.deepEqual(await tasks.deleteBatch([]), []);
console.log("deleteBatch([a, b, 999999]) -> [a, b]; archiveBatch([c]) -> [c]");

// pop reads and deletes in one statement: at-most-once, no ack needed (or possible).
await tasks.setVt(d, 0);
const [popped] = await tasks.pop();
assert.equal(popped?.msgId, d);
assert.equal((await tasks.metrics()).queueLength, 0n);
console.log(`pop: got message ${d} and it is already gone from the queue`);

// A delayed batch stays invisible until its delay has passed.
await tasks.sendBatch([{ task: "later" }, { task: "later too" }], { delay: 60 });
const metrics = await tasks.metrics();
assert.equal(metrics.queueLength, 2n);
assert.equal(metrics.queueVisibleLength, 0n);
console.log("sendBatch with delay 60: 2 messages queued, 0 visible");

await tasks.drop();
await sql.close();
console.log("ok");
