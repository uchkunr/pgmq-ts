// The README quickstart: create a queue, send a message, read it, delete it.
//
//   scripts/db.sh start
//   bun examples/quickstart.ts

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
