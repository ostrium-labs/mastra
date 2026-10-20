import { MastraError } from '@mastra/core/error';
import { describe, expect, it } from 'vitest';
import { CodecError, decodeError, decodeValue, encodeError, encodeValue } from './codec';

describe('codec', () => {
  it('codec_round_trips_errors', () => {
    const root = new TypeError('connection reset');
    const error = Object.assign(new Error('model call failed', { cause: root }), {
      statusCode: 429,
      responseHeaders: { 'retry-after': '2' },
    });
    const wire = JSON.parse(JSON.stringify(encodeError(error)));
    const back = decodeError(wire) as Error & {
      statusCode?: number;
      responseHeaders?: Record<string, string>;
    };
    expect(back).toBeInstanceOf(Error);
    expect(back.message).toBe('model call failed');
    expect(back.statusCode).toBe(429);
    expect(back.responseHeaders).toEqual({ 'retry-after': '2' });
    expect((back.cause as Error).message).toBe('connection reset');
    expect(back.stack).toContain('model call failed');
  });

  it('keeps errors nested in step results', () => {
    const value = {
      status: 'failed',
      steps: {
        a: {
          status: 'failed',
          error: new MastraError({ id: 'X', domain: 'TOOL', category: 'USER', text: 'bad' } as any),
        },
      },
    };
    const back = decodeValue<any>(JSON.parse(JSON.stringify(encodeValue(value))));
    expect(back.steps.a.error).toBeInstanceOf(Error);
    expect(back.steps.a.error.message).toBe('bad');
  });

  it('round-trips dates and encodes like JSON for undefined and non-finite numbers', () => {
    const date = new Date('2026-10-20T00:00:00.000Z');
    const wire = JSON.parse(
      JSON.stringify(encodeValue({ d: date, u: undefined, n: Number.NaN, list: [undefined, 1] })),
    );
    expect(wire).toEqual({
      d: { __mastraResonateDate: '2026-10-20T00:00:00.000Z' },
      n: null,
      list: [null, 1],
    });
    const back = decodeValue<{ d: Date }>(wire);
    expect(back.d).toBeInstanceOf(Date);
    expect(back.d.getTime()).toBe(date.getTime());
    expect(encodeValue(new Date(Number.NaN))).toBeNull();
    expect(encodeValue(undefined)).toBeUndefined();
  });

  it('refuses functions, symbols, bigints and cycles with a typed error', () => {
    expect(() => encodeValue({ f: () => 1 })).toThrow(CodecError);
    expect(() => encodeValue({ s: Symbol('x') })).toThrow(CodecError);
    expect(() => encodeValue({ b: 1n })).toThrow(CodecError);
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => encodeValue(cyclic)).toThrow(/cyclic value at self/);
    // A shared (non-cyclic) reference is fine.
    const shared = { x: 1 };
    expect(encodeValue({ a: shared, b: shared })).toEqual({ a: { x: 1 }, b: { x: 1 } });
  });
});
