import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  getCompletedCount,
  getCompletedPercent,
  getProgress,
  isCompleted,
  lessonKey,
  markCompleted,
  PROGRESS_STORAGE_KEY,
  unmarkCompleted,
} from './progress';

// jsdom in this project ships without a working Storage implementation,
// so install a minimal in-memory shim shared across the test file.
beforeAll(() => {
  if (typeof window.localStorage?.setItem === 'function') return;
  const store = new Map<string, string>();
  const shim: Storage = {
    get length() {
      return store.size;
    },
    clear: () => store.clear(),
    getItem: (key: string) => (store.has(key) ? store.get(key)! : null),
    setItem: (key: string, value: string) => void store.set(key, String(value)),
    removeItem: (key: string) => void store.delete(key),
    key: (index: number) => Array.from(store.keys())[index] ?? null,
  };
  Object.defineProperty(window, 'localStorage', {
    configurable: true,
    value: shim,
  });
});

beforeEach(() => {
  window.localStorage.clear();
});

describe('lessonKey', () => {
  it('joins moduleId and slug with slash', () => {
    expect(lessonKey('02-producer', '02-03-idempotent-producer')).toBe(
      '02-producer/02-03-idempotent-producer',
    );
  });
});

describe('getProgress', () => {
  it('returns empty object when storage is empty', () => {
    expect(getProgress()).toEqual({});
  });

  it('returns empty object on unparseable JSON', () => {
    window.localStorage.setItem(PROGRESS_STORAGE_KEY, '{not json');
    expect(getProgress()).toEqual({});
  });

  it('drops malformed entries but keeps valid ones', () => {
    window.localStorage.setItem(
      PROGRESS_STORAGE_KEY,
      JSON.stringify({
        'a/b': { completed: true, at: '2026-05-01T10:00:00.000Z' },
        'c/d': { completed: false, at: '2026-05-01T10:00:00.000Z' },
        'e/f': 'nope',
        'g/h': { completed: true },
      }),
    );
    const map = getProgress();
    expect(Object.keys(map)).toEqual(['a/b']);
    expect(map['a/b' as keyof typeof map]).toEqual({
      completed: true,
      at: '2026-05-01T10:00:00.000Z',
    });
  });
});

describe('markCompleted / unmarkCompleted / isCompleted', () => {
  it('marks a lesson with the given timestamp', () => {
    const key = lessonKey('01-foundations', '01-01-architecture-and-kraft');
    const now = '2026-05-06T12:00:00.000Z';
    const map = markCompleted(key, () => now);
    expect(map[key]).toEqual({ completed: true, at: now });
    expect(isCompleted(map, key)).toBe(true);
    expect(getProgress()[key]).toEqual({ completed: true, at: now });
  });

  it('overwrites the timestamp on re-mark', () => {
    const key = lessonKey('01-foundations', '01-01-architecture-and-kraft');
    markCompleted(key, () => '2026-05-01T00:00:00.000Z');
    const map = markCompleted(key, () => '2026-05-06T00:00:00.000Z');
    expect(map[key]?.at).toBe('2026-05-06T00:00:00.000Z');
  });

  it('unmarks a previously completed lesson', () => {
    const key = lessonKey('02-producer', '02-03-idempotent-producer');
    markCompleted(key, () => '2026-05-06T00:00:00.000Z');
    const after = unmarkCompleted(key);
    expect(after[key]).toBeUndefined();
    expect(isCompleted(after, key)).toBe(false);
    expect(getProgress()[key]).toBeUndefined();
  });

  it('unmark is a no-op for unknown keys', () => {
    const map = unmarkCompleted(lessonKey('x', 'y'));
    expect(map).toEqual({});
  });
});

describe('getCompletedCount / getCompletedPercent', () => {
  it('counts completed lessons', () => {
    markCompleted(lessonKey('a', 'b'), () => '2026-05-06T00:00:00.000Z');
    markCompleted(lessonKey('c', 'd'), () => '2026-05-06T00:00:00.000Z');
    expect(getCompletedCount(getProgress())).toBe(2);
  });

  it('computes percent against the provided total', () => {
    expect(getCompletedPercent({}, 42)).toBe(0);
    for (let i = 0; i < 21; i += 1) {
      markCompleted(lessonKey('m', String(i)), () => '2026-05-06T00:00:00.000Z');
    }
    expect(getCompletedPercent(getProgress(), 42)).toBe(50);
  });

  it('returns 0 when total is zero or negative', () => {
    markCompleted(lessonKey('a', 'b'), () => '2026-05-06T00:00:00.000Z');
    expect(getCompletedPercent(getProgress(), 0)).toBe(0);
    expect(getCompletedPercent(getProgress(), -1)).toBe(0);
  });

  it('clamps percent to 100 when count exceeds total (orphan keys)', () => {
    for (let i = 0; i < 5; i += 1) {
      markCompleted(lessonKey('m', String(i)), () => '2026-05-06T00:00:00.000Z');
    }
    expect(getCompletedPercent(getProgress(), 3)).toBe(100);
  });
});
