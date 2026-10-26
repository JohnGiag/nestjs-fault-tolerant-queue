/** Knobs that make the simulated handler misbehave on purpose, to exercise retry and DLQ paths. */
export interface SimulationOptions {
  /** Throw a transient error on the first N attempts, then succeed. */
  failTimes?: number;
  /** Throw an UnrecoverableError: skips remaining retries and goes straight to the DLQ. */
  poison?: boolean;
  /** Simulated work duration. */
  durationMs?: number;
}

/** Data stored on every job in the primary queue. */
export interface TaskPayload {
  /** Task kind; also used as the BullMQ job name. */
  type: string;
  data: Record<string, unknown>;
  simulate?: SimulationOptions;
}

/** Per-job overrides a caller may set; everything else comes from the queue defaults. */
export interface TaskJobOptions {
  priority?: number;
  delay?: number;
  attempts?: number;
}

export interface TaskResult {
  type: string;
  processedAt: string;
}

/** What lands in the DLQ: enough to diagnose the failure and replay the original payload. */
export interface DeadLetterRecord {
  originalJobId: string;
  originalQueue: string;
  jobName: string;
  payload: TaskPayload;
  failedReason: string;
  /** Stack of the final failure. */
  stacktrace: string | undefined;
  /** Stacks of every failed attempt, oldest first, as recorded by BullMQ. */
  attemptStacktraces: string[];
  attemptsMade: number;
  maxAttempts: number;
  /** The job's own options, so a replay can restore them (BullMQ keeps them only on the dead job). */
  jobOptions: TaskJobOptions;
  enqueuedAt: string;
  failedAt: string;
}
