import { BadRequestException, PayloadTooLargeException } from '@nestjs/common';
import type {
  SimulationOptions,
  TaskJobOptions,
  TaskPayload,
} from '../queue/interfaces/task.interface';

export interface CreateTaskDto {
  payload: TaskPayload;
  options: TaskJobOptions;
}

// Caps on caller-controlled job options: unbounded values would let one request pin a worker
// (huge failTimes/duration) or park jobs in Redis indefinitely (huge delay).
const MAX_ATTEMPTS = 10;
const MAX_DELAY_MS = 24 * 60 * 60 * 1000;
const MAX_DURATION_MS = 60_000;
const MAX_PRIORITY = 2_097_152; // BullMQ's upper bound
// Every job is stored in Redis (noeviction) and copied to the DLQ if it dies, so size is capped.
const MAX_DATA_BYTES = 64 * 1024;

const isObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v);

function optionalInt(
  obj: Record<string, unknown>,
  field: string,
  min: number,
  max: number,
): number | undefined {
  const v = obj[field];
  if (v === undefined) return undefined;
  if (!Number.isInteger(v) || (v as number) < min || (v as number) > max) {
    throw new BadRequestException(
      `${field} must be an integer between ${min} and ${max}`,
    );
  }
  return v as number;
}

/** Validates an untrusted request body into a typed payload; throws 400 on anything unexpected. */
export function parseCreateTask(body: unknown): CreateTaskDto {
  if (!isObject(body))
    throw new BadRequestException('Body must be a JSON object');

  // simulate makes workers sleep and fail on purpose: a free DoS lever if the public can set it.
  // Opt-in by environment, so a deploy that forgets NODE_ENV stays safe.
  const canSimulate = ['development', 'test'].includes(
    process.env.NODE_ENV ?? '',
  );
  if (body.simulate !== undefined && !canSimulate) {
    throw new BadRequestException(
      'simulate is only allowed when NODE_ENV is development or test',
    );
  }

  const { type, data = {}, simulate = {}, options = {} } = body;
  if (typeof type !== 'string' || !/^[\w.-]{1,100}$/.test(type)) {
    throw new BadRequestException(
      'type must be 1-100 characters of letters, digits, "_", "-" or "."',
    );
  }
  if (!isObject(data)) throw new BadRequestException('data must be an object');
  if (Buffer.byteLength(JSON.stringify(data)) > MAX_DATA_BYTES) {
    throw new PayloadTooLargeException(
      `data must be at most ${MAX_DATA_BYTES} bytes`,
    );
  }
  if (!isObject(simulate))
    throw new BadRequestException('simulate must be an object');
  if (!isObject(options))
    throw new BadRequestException('options must be an object');
  if (simulate.poison !== undefined && typeof simulate.poison !== 'boolean') {
    throw new BadRequestException('simulate.poison must be a boolean');
  }

  const sim: SimulationOptions = {
    failTimes: optionalInt(simulate, 'failTimes', 0, MAX_ATTEMPTS),
    durationMs: optionalInt(simulate, 'durationMs', 0, MAX_DURATION_MS),
    poison: simulate.poison,
  };
  return {
    payload: { type, data, simulate: sim },
    options: {
      attempts: optionalInt(options, 'attempts', 1, MAX_ATTEMPTS),
      delay: optionalInt(options, 'delay', 0, MAX_DELAY_MS),
      priority: optionalInt(options, 'priority', 0, MAX_PRIORITY),
    },
  };
}
