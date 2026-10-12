// Graceful shutdown: aborting the signal stops polling, waits for running handlers, then
// resolves. If handlers outlive shutdownTimeoutMs, their signal is aborted and run() resolves;
// their messages are not lost, they are delivered again after the visibility timeout.
//
//   bun examples/worker-shutdown.ts   (takes about 3 seconds)

import assert from "node:assert/strict";
import { SQL } from "bun";
import { Pgmq, Worker, type WorkerStage } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:54329/postgres");
const pgmq = new Pgmq(sql);

const jobs = pgmq.queue<{ n: number }>("example_worker_shutdown");
await jobs.drop();
await jobs.create();

// 1. Shut down while 2 slow jobs are running and 3 more are waiting.
await jobs.sendBatch([1, 2, 3, 4, 5].map((n) => ({ n })));

const started: number[] = [];
const finished: number[] = [];
const worker = new Worker<{ n: number }>(sql, {
  queue: jobs.name,
  vt: 30,
  concurrency: 2,
  pollIntervalMs: 50,
  handler: async (message) => {
    started.push(message.message.n);
    await Bun.sleep(1_000);
    finished.push(message.message.n);
  },
});

const controller = new AbortController();
const running = worker.run({ signal: controller.signal });
while (started.length < 2) await Bun.sleep(10);

const stopAt = performance.now();
controller.abort();
await running;
const stoppedAfter = Math.round(performance.now() - stopAt);

assert.deepEqual(finished.sort(), [1, 2]);
assert(stoppedAfter >= 800, `run() resolved after ${stoppedAfter} ms, before the handlers finished`);
const afterFirst = await jobs.metrics();
assert.equal(afterFirst.queueLength, 3n);
assert.equal(afterFirst.queueVisibleLength, 3n);
console.log(`1. abort with jobs 1 and 2 running: run() waited ${stoppedAfter} ms for them, and both were acknowledged`);
console.log("   jobs 3-5 were never read: still in the queue and visible to the next worker");

// 2. A handler slower than shutdownTimeoutMs: its signal is aborted and run() stops waiting.
await jobs.purge();
await jobs.send({ n: 6 });

const stages: WorkerStage[] = [];
let handlerSawAbort = false;
let handlerStarted = false;
const impatient = new Worker(sql, {
  queue: jobs.name,
  vt: 30,
  pollIntervalMs: 50,
  shutdownTimeoutMs: 300,
  onError: (_error, { stage }) => {
    stages.push(stage);
  },
  handler: async (_message, { signal }) => {
    handlerStarted = true;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, 10_000);
      signal.addEventListener("abort", () => {
        clearTimeout(timer);
        handlerSawAbort = true;
        reject(signal.reason);
      });
    });
  },
});

const second = new AbortController();
const secondRun = impatient.run({ signal: second.signal });
while (!handlerStarted) await Bun.sleep(10);

const secondStopAt = performance.now();
second.abort();
await secondRun;
const secondStoppedAfter = Math.round(performance.now() - secondStopAt);

assert(secondStoppedAfter < 1_000, `took ${secondStoppedAfter} ms`);
assert(handlerSawAbort);
assert(stages.includes("shutdown"));
const afterSecond = await jobs.metrics();
assert.equal(afterSecond.queueLength, 1n);
assert.equal(afterSecond.queueVisibleLength, 0n);
console.log(`2. shutdownTimeoutMs 300 with a 10 s job: run() resolved after ${secondStoppedAfter} ms`);
console.log("   the handler's signal was aborted; its message is still queued and will be redelivered after vt");

await jobs.drop();
await sql.close();
console.log("ok");
