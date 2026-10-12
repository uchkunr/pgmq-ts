/**
 * Anything that can run one parameterized query as a tagged template.
 *
 * Every Bun SQL handle satisfies it: the client (`new SQL(url)`), a transaction (the `tx` passed
 * to `sql.begin`), a savepoint, and a reserved connection (`await sql.reserve()`). pgmq-ts runs
 * each operation on the executor it is given and nothing else: it never opens a connection,
 * never begins, commits or rolls back a transaction, and never closes the client.
 *
 * The library only ever calls it as a tagged template, so every value is a bound parameter.
 */
export interface Executor {
  (strings: TemplateStringsArray, ...values: unknown[]): PromiseLike<unknown>;
}

/**
 * An {@link Executor} that can also open a transaction. The {@link Worker} needs one, because it
 * moves a message to the dead-letter queue (send + delete) inside a single transaction.
 *
 * Bun's `SQL` client and reserved connections satisfy it.
 */
export interface TransactionalExecutor extends Executor {
  begin(fn: (tx: Executor) => Promise<void>): PromiseLike<unknown>;
}
