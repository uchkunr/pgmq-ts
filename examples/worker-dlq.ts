// A Worker with retries, exponential backoff, and a dead-letter queue.
//
// - a payment that fails once succeeds on its second attempt
// - payments that always fail are retried with backoff, then moved to the dead-letter queue
// - a message the parse function rejects counts as a failed attempt, like a handler error
// - the dead-letter move is atomic and keeps the body exactly as stored, even numbers that
//   JavaScript cannot represent
//
//   bun examples/worker-dlq.ts   (takes about 6 seconds)

import assert from "node:assert/strict";
import { SQL } from "bun";
import { Pgmq, Worker, type WorkerStage } from "pgmq-ts";

const sql = new SQL(process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:54329/postgres");
const pgmq = new Pgmq(sql);

type Payment = { id: string; behavior: "ok" | "flaky" | "declined" };

function parsePayment(raw: unknown): Payment {
  if (
    typeof raw === "object" && raw !== null &&
    "id" in raw && typeof raw.id === "string" &&
    "behavior" in raw && (raw.behavior === "ok" || raw.behavior === "flaky" || raw.behavior === "declined")
  ) {
    return { id: raw.id, behavior: raw.behavior };
  }
  throw new Error("not a payment");
}

const payments = pgmq.queue("example_payments", { parse: parsePayment });
const deadLetters = pgmq.queue("example_payments_dlq");
for (const queue of [payments, deadLetters]) {
  await queue.drop();
  await queue.create();
}

await payments.sendBatch([
  { id: "p1", behavior: "ok" },
  { id: "p2", behavior: "flaky" },
  { id: "p3", behavior: "declined" },
]);
// Not a Payment: parse rejects it. Sent through an unparsed handle, with a number that does not
// fit a JavaScript double, so we can check the dead-letter copy is exact.
await sql`SELECT pgmq.send('example_payments', '{"amount": 12345678901234567891}'::jsonb, '{"source": "legacy"}'::jsonb)`;

const handled: string[] = [];
const attempts = new Map<string, number[]>();
const errors: WorkerStage[] = [];

const worker = new Worker(sql, {
  queue: payments.name,
  parse: parsePayment,
  vt: 10,
  concurrency: 2,
  pollIntervalMs: 100,
  maxAttempts: 3,
  backoff: { baseSeconds: 1, maxSeconds: 2 }, // retry after 1 s, then 2 s
  deadLetterQueue: deadLetters.name,
  onError: (_error, { stage }) => {
    errors.push(stage);
  },
  handler: async (message) => {
    const { id, behavior } = message.message;
    attempts.set(id, [...(attempts.get(id) ?? []), message.readCount]);
    if (behavior === "declined") throw new Error(`payment ${id} declined`);
    if (behavior === "flaky" && message.readCount === 1) throw new Error(`payment ${id}: gateway timeout`);
    handled.push(id);
  },
});

const controller = new AbortController();
const running = worker.run({ signal: controller.signal });

// Wait until the source queue is empty and both bad messages are dead-lettered.
const deadline = Date.now() + 20_000;
while (true) {
  const [source, dlq] = await Promise.all([payments.metrics(), deadLetters.metrics()]);
  if (source.queueLength === 0n && dlq.queueLength === 2n) break;
  assert(Date.now() < deadline, "timed out waiting for the worker");
  await Bun.sleep(100);
}
controller.abort();
await running;

assert.deepEqual(handled.sort(), ["p1", "p2"]);
assert.deepEqual(attempts.get("p2"), [1, 2]);
assert.deepEqual(attempts.get("p3"), [1, 2, 3]);
console.log("p1 succeeded at once; p2 failed once and succeeded on attempt 2");
console.log("p3 was declined on attempts", attempts.get("p3")?.join(", "), "then dead-lettered (attempt 4 > maxAttempts 3)");

// 3 handler failures for p3, 3 parse failures for the malformed message, 1 for flaky p2.
assert.equal(errors.filter((stage) => stage === "handle").length, 7);
assert.equal(errors.filter((stage) => stage !== "handle").length, 0);
console.log("onError saw 7 failed attempts (stage \"handle\") and nothing else");

const dead: { message: string; headers: string | null }[] = await sql`
  SELECT message::text AS message, headers::text AS headers FROM pgmq.q_example_payments_dlq ORDER BY msg_id`;
assert.deepEqual(
  dead.map((row) => row.message).sort(),
  ['{"amount": 12345678901234567891}', '{"id": "p3", "behavior": "declined"}'],
);
assert(dead.some((row) => row.headers === '{"source": "legacy"}'));
console.log("dead-letter queue holds p3 and the malformed message, bodies and headers byte for byte:");
for (const row of dead) console.log("  ", row.message, row.headers ?? "");

for (const queue of [payments, deadLetters]) await queue.drop();
await sql.close();
console.log("ok");
