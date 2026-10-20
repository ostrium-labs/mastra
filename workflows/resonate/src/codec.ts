import { getErrorFromUnknown } from '@mastra/core/error';

/**
 * JSON-safe encoding for everything the engine stores in a durable promise:
 * step results, operation values, workflow results and errors.
 *
 * Resonate's own codec runs `JSON.stringify` and keeps only `name`, `message`
 * and `stack` of an `Error`, so a step failure would lose its `cause` chain and
 * custom properties (`statusCode`, `responseHeaders`, ...). Values are encoded
 * here first, into plain JSON, and decoded on the way out.
 *
 * Errors become `{ [ERROR_TAG]: true, ...serialized }` using Mastra's own
 * serializer (`getErrorFromUnknown(...).toJSON()`), and are revived with
 * `getErrorFromUnknown`, so they round-trip the way Mastra's snapshots do.
 *
 * Dates become `{ [DATE_TAG]: iso }` and come back as `Date`s (plain JSON would
 * turn them into strings, which a step's `z.date()` input schema then rejects).
 *
 * Values JSON cannot carry faithfully are refused with a {@link CodecError}
 * instead of being dropped: functions, symbols, bigints and cycles.
 * `undefined` object properties are dropped, as JSON does.
 */

const ERROR_TAG = '__mastraResonateError';
const DATE_TAG = '__mastraResonateDate';

export class CodecError extends Error {
  readonly path: string;

  constructor(message: string, path: string) {
    super(`${message} at ${path || '<root>'}`);
    this.name = 'CodecError';
    this.path = path;
  }
}

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

/** Encodes `value` into plain JSON. Throws {@link CodecError} on values JSON cannot carry. */
export function encodeValue(value: unknown): Json | undefined {
  if (value === undefined) return undefined;
  return encode(value, '', new Set());
}

/** Decodes a value produced by {@link encodeValue}, reviving errors. */
export function decodeValue<T = unknown>(value: unknown): T {
  return decode(value) as T;
}

/** Encodes an error (or any thrown value) into its JSON-safe form. */
export function encodeError(error: unknown): Json {
  const instance = getErrorFromUnknown(error, {
    serializeStack: true,
    fallbackMessage: 'Unknown error',
  });
  const serialized = encode(instance.toJSON(), '', new Set());
  return { ...(serialized as Record<string, Json>), [ERROR_TAG]: true };
}

/** Revives an error produced by {@link encodeError}. */
export function decodeError(value: unknown): Error {
  if (value instanceof Error) return value;
  const decoded = decode(value);
  return decoded instanceof Error
    ? decoded
    : getErrorFromUnknown(decoded, { serializeStack: false, fallbackMessage: 'Unknown error' });
}

function encode(value: unknown, path: string, ancestors: Set<object>): Json {
  if (value === null) return null;
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return value;
    case 'number':
      // JSON turns NaN and the infinities into null; keep that behaviour.
      return Number.isFinite(value) ? value : null;
    case 'undefined':
      return null;
    case 'function':
      throw new CodecError('Cannot encode a function', path);
    case 'symbol':
      throw new CodecError('Cannot encode a symbol', path);
    case 'bigint':
      throw new CodecError('Cannot encode a bigint', path);
  }

  const obj = value as object;
  if (ancestors.has(obj)) {
    throw new CodecError('Cannot encode a cyclic value', path);
  }

  if (obj instanceof Error) {
    return encodeError(obj);
  }

  if (obj instanceof Date) {
    return Number.isNaN(obj.getTime()) ? null : { [DATE_TAG]: obj.toISOString() };
  }

  ancestors.add(obj);
  try {
    // URLs and anything else with toJSON behave as they do under JSON.stringify.
    const toJSON = (obj as { toJSON?: unknown }).toJSON;
    if (typeof toJSON === 'function') {
      return encode(toJSON.call(obj), path, ancestors);
    }

    if (Array.isArray(obj)) {
      return obj.map((item, index) => {
        const itemPath = `${path}[${index}]`;
        // JSON writes null for undefined, functions and symbols in arrays; only
        // undefined is accepted here, the others are refused like anywhere else.
        return item === undefined ? null : encode(item, itemPath, ancestors);
      });
    }

    if (obj instanceof Map || obj instanceof Set) {
      throw new CodecError(`Cannot encode a ${obj instanceof Map ? 'Map' : 'Set'}`, path);
    }

    const out: Record<string, Json> = {};
    for (const [key, entry] of Object.entries(obj)) {
      if (entry === undefined) continue;
      out[key] = encode(entry, path ? `${path}.${key}` : key, ancestors);
    }
    return out;
  } finally {
    ancestors.delete(obj);
  }
}

function decode(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(decode);

  const record = value as Record<string, unknown>;
  if (typeof record[DATE_TAG] === 'string' && Object.keys(record).length === 1) {
    return new Date(record[DATE_TAG] as string);
  }
  const out: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(record)) {
    if (key === ERROR_TAG) continue;
    out[key] = decode(entry);
  }
  if (record[ERROR_TAG] === true) {
    return getErrorFromUnknown(out, { serializeStack: false, fallbackMessage: 'Unknown error' });
  }
  return out;
}
