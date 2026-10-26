export const DEFAULT_QUEUE = 'default-jobs';
export const DEFAULT_DLQ = 'default-jobs-dlq';

/** How long a repeated Idempotency-Key request returns the original job instead of a new one. */
export const IDEMPOTENCY_TTL_MS = 24 * 60 * 60 * 1000;
