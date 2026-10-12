// readWithPoll waits for a message instead of returning empty-handed, and stops the moment its
// AbortSignal fires.
//
//   bun examples/read-with-poll.ts

import assert from "node:assert/strict";
import { SQL } from "bun";
import { Pgmq } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:54329/postgres");
const pgmq = new Pgmq(sql);

const inbox = pgmq.queue<{ n: number }>("example_read_with_poll");
await inbox.drop();
await inbox.create();

const since = (start: number): number => Math.round(performance.now() - start);

// 1. Empty queue: keeps trying for maxPollSeconds, then returns an empty array.
let start = performance.now();
assert.deepEqual(await inbox.readWithPoll({ vt: 30, maxPollSeconds: 1 }), []);
const waited = since(start);
assert(waited >= 950 && waited < 2_000, `waited ${waited} ms`);
console.log(`1. empty queue: returned [] after ${waited} ms (maxPollSeconds: 1)`);

// 2. A message that arrives while polling is returned right away.
start = performance.now();
const producer = Bun.sleep(300).then(() => inbox.send({ n: 1 }));
const [message] = await inbox.readWithPoll({ vt: 30, maxPollSeconds: 10, pollIntervalMs: 50 });
await producer;
assert.equal(message?.message.n, 1);
const arrival = since(start);
assert(arrival < 1_500, `took ${arrival} ms`);
console.log(`2. a message sent 300 ms into the poll was returned after ${arrival} ms`);
await inbox.delete(message.msgId);

// 3. Aborting stops the poll at once, even with 30 seconds of polling left.
const controller = new AbortController();
setTimeout(() => controller.abort(), 200);
start = performance.now();
await assert.rejects(inbox.readWithPoll({ vt: 30, maxPollSeconds: 30, signal: controller.signal }), {
  name: "AbortError",
});
const aborted = since(start);
assert(aborted < 1_000, `took ${aborted} ms`);
console.log(`3. AbortController.abort() after 200 ms: rejected with AbortError after ${aborted} ms`);

// 4. AbortSignal.timeout works the same way and rejects with a TimeoutError.
start = performance.now();
await assert.rejects(inbox.readWithPoll({ vt: 30, maxPollSeconds: 30, signal: AbortSignal.timeout(250) }), {
  name: "TimeoutError",
});
console.log(`4. AbortSignal.timeout(250): rejected with TimeoutError after ${since(start)} ms`);

await inbox.drop();
await sql.close();
console.log("ok");
