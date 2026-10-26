import { BadRequestException, PayloadTooLargeException } from '@nestjs/common';
import { parseCreateTask } from './create-task.dto';

describe('parseCreateTask', () => {
  afterEach(() => {
    process.env.NODE_ENV = 'test';
  });

  const simulated = { type: 'x', simulate: { failTimes: 1 } };

  it.each(['development', 'test'])(
    'accepts simulate when NODE_ENV=%s',
    (env) => {
      process.env.NODE_ENV = env;
      expect(parseCreateTask(simulated).payload.simulate?.failTimes).toBe(1);
    },
  );

  it.each(['production', 'staging', 'prod', '', undefined])(
    'rejects simulate when NODE_ENV=%p',
    (env) => {
      if (env === undefined) delete process.env.NODE_ENV;
      else process.env.NODE_ENV = env;
      expect(() => parseCreateTask(simulated)).toThrow(BadRequestException);
      expect(parseCreateTask({ type: 'x' }).payload.type).toBe('x');
    },
  );

  it('rejects data over 64 KB', () => {
    const body = { type: 'x', data: { blob: 'a'.repeat(64 * 1024) } };
    expect(() => parseCreateTask(body)).toThrow(PayloadTooLargeException);
  });

  it('accepts a minimal body and defaults data to {}', () => {
    expect(parseCreateTask({ type: 'email' }).payload).toMatchObject({
      type: 'email',
      data: {},
    });
  });

  it('passes through bounded job options', () => {
    expect(
      parseCreateTask({
        type: 'x',
        options: { attempts: 5, delay: 0, priority: 1 },
      }).options,
    ).toEqual({ attempts: 5, delay: 0, priority: 1 });
  });

  it.each([
    ['non-object body', 'nope'],
    ['missing type', {}],
    ['type with ":"', { type: 'a:b' }],
    ['array data', { type: 'x', data: [] }],
    ['attempts above cap', { type: 'x', options: { attempts: 11 } }],
    ['fractional delay', { type: 'x', options: { delay: 1.5 } }],
    ['string failTimes', { type: 'x', simulate: { failTimes: '2' } }],
    ['non-boolean poison', { type: 'x', simulate: { poison: 'yes' } }],
  ])('rejects %s', (_, body) => {
    expect(() => parseCreateTask(body)).toThrow(BadRequestException);
  });
});
