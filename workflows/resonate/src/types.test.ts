import { describe, expect, it } from 'vitest';
import { detectGroup, firstPassId, resolveConfig, rootIdForRun, statusFromPromiseState } from './types';

describe('types', () => {
  it('status_maps_each_state', () => {
    expect(statusFromPromiseState('pending')).toBe('running');
    expect(statusFromPromiseState('resolved')).toBe('success');
    expect(statusFromPromiseState('rejected')).toBe('failed');
    expect(statusFromPromiseState('rejected_canceled')).toBe('canceled');
    expect(statusFromPromiseState('rejected_timedout')).toBe('failed');
  });

  it('encodes run ids into root ids without the reserved colon', () => {
    expect(rootIdForRun('3f1c0a7e-1b2c')).toBe('3f1c0a7e-1b2c');
    expect(rootIdForRun('tenant:42%x')).toBe('tenant%3A42%25x');
    expect(firstPassId('abc')).toBe('abc:0');
    expect(() => rootIdForRun('')).toThrow();
  });

  it('resolves the worker group from the client and refuses a mismatch', () => {
    const client = { network: { anycast: 'poll://any@workers/pid1' } };
    expect(detectGroup(client)).toBe('workers');
    expect(detectGroup({})).toBeUndefined();
    expect(resolveConfig({}, 'workers').group).toBe('workers');
    expect(resolveConfig({}).group).toBe('default');
    expect(() => resolveConfig({ group: 'other' }, 'workers')).toThrow(/differs/);
    expect(() => resolveConfig({ timeout: 0 })).toThrow(/timeout/);
  });
});
