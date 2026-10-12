// A read hides a message for `vt` seconds. If it is not deleted or archived in that time,
// it is delivered again, with readCount one higher. This is what makes delivery at-least-once.
//
//   bun examples/visibility-timeout.ts

import assert from "node:assert/strict";
import { SQL } from "bun";
import { Pgmq } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:54329/postgres");
const pgmq = new Pgmq(sql);

const jobs = pgmq.queue<{ task: string }>("example_visibility_timeout");
await jobs.drop();
await jobs.create();

const msgId = await jobs.send({ task: "resize-image" });

const [first] = await jobs.read({ vt: 2 });
assert.equal(first?.msgId, msgId);
assert.equal(first.readCount, 1);
console.log(`read message ${msgId} (readCount 1); it is hidden until ${first.vt.toISOString()}`);

assert.deepEqual(await jobs.read({ vt: 2 }), []);
console.log("a second read right away gets nothing: the message is invisible");

// Pretend the consumer crashed and never deleted it. Wait out the visibility timeout.
await Bun.sleep(2_500);

const [again] = await jobs.read({ vt: 30 });
assert.equal(again?.msgId, msgId);
assert.equal(again.readCount, 2);
console.log("after 2 s the same message is delivered again, readCount 2");

// Handled this time: archive it so it is kept for inspection but never delivered again.
assert.equal(await jobs.archive(again.msgId), true);
assert.equal((await jobs.metrics()).queueLength, 0n);
const [archived]: { n: number }[] = await sql`SELECT count(*)::int AS n FROM pgmq.a_example_visibility_timeout`;
assert.equal(archived?.n, 1);
console.log("archived: the queue is empty and the message is in pgmq.a_example_visibility_timeout");

await jobs.drop();
await sql.close();
console.log("ok");
