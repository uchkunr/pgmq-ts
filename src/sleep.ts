/**
 * Resolves after `ms` milliseconds, or rejects with `signal.reason` as soon as `signal` aborts.
 *
 * `Bun.sleep` cannot be cancelled: its timer would keep the process alive after a worker has
 * stopped. A cleared `setTimeout` does not.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal === undefined) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
  if (signal.aborted) {
    return Promise.reject(signal.reason);
  }
  return new Promise((resolve, reject) => {
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
