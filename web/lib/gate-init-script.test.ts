import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { parseCourse, type Course } from './course';
import { buildGateInitScript, GATE_LOCKED_ATTR } from './gate-init-script';
import { FURTHEST_STORAGE_KEY, PROGRESS_STORAGE_KEY, lessonKey } from './progress';

const COURSE_YAML = `
title: Test
description: Test
basePath: /test
repoUrl: https://example.com/test
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
      - slug: 01-02-deep
        title: Deep
        duration: 45m
  - id: 02-bar
    title: Bar
    description: D
    lessons:
      - slug: 02-01-start
        title: Start
        duration: 20m
      - slug: 02-02-end
        title: End
        duration: 20m
`;

function makeCourse(): Course {
  return parseCourse(COURSE_YAML);
}

type FakeWindow = {
  location: { pathname: string };
  localStorage: Storage;
};

function runScript(script: string, fakeWindow: FakeWindow): void {
  const fn = new Function('window', script);
  fn(fakeWindow);
}

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
  document.documentElement.removeAttribute(GATE_LOCKED_ATTR);
});

afterEach(() => {
  document.documentElement.removeAttribute(GATE_LOCKED_ATTR);
});

function fakeWindow(pathname: string): FakeWindow {
  return {
    location: { pathname },
    localStorage: window.localStorage,
  };
}

describe('buildGateInitScript', () => {
  it('does not lock when the path has fewer than 2 segments', () => {
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/'));
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('locks a far-future lesson on a pre-i18n path', () => {
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/02-bar/02-02-end'));
    expect(document.documentElement.getAttribute(GATE_LOCKED_ATTR)).toBe('true');
  });

  it('locks a far-future lesson under the /ru/ prefix', () => {
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/ru/02-bar/02-02-end'));
    expect(document.documentElement.getAttribute(GATE_LOCKED_ATTR)).toBe('true');
  });

  it('locks a far-future lesson under the /en/ prefix', () => {
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/en/02-bar/02-02-end'));
    expect(document.documentElement.getAttribute(GATE_LOCKED_ATTR)).toBe('true');
  });

  it('does not lock the first lesson under /ru/ prefix', () => {
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/ru/01-foo/01-01-intro'));
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('does not lock /ru/ root (no module/slug after strip)', () => {
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/ru/'));
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('strips basePath then strips lang prefix', () => {
    const script = buildGateInitScript(makeCourse(), '/kafka-cookbook');
    runScript(script, fakeWindow('/kafka-cookbook/ru/02-bar/02-02-end'));
    expect(document.documentElement.getAttribute(GATE_LOCKED_ATTR)).toBe('true');
  });

  it('respects the furthest pointer in localStorage for /en/ paths', () => {
    window.localStorage.setItem(
      FURTHEST_STORAGE_KEY,
      lessonKey('02-bar', '02-01-start'),
    );
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/en/02-bar/02-02-end'));
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('respects completed lessons in progress storage for /ru/ paths', () => {
    const progress = {
      [lessonKey('01-foo', '01-01-intro')]: { completed: true, at: 'x' },
      [lessonKey('01-foo', '01-02-deep')]: { completed: true, at: 'x' },
    };
    window.localStorage.setItem(PROGRESS_STORAGE_KEY, JSON.stringify(progress));
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/ru/02-bar/02-01-start'));
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('does not lock for an unknown moduleId/slug under /en/', () => {
    const script = buildGateInitScript(makeCourse(), '');
    runScript(script, fakeWindow('/en/ghost/missing'));
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('does not strip a partial-prefix like /enfoo', () => {
    const script = buildGateInitScript(makeCourse(), '');
    // /enfoo/something — first segment is "enfoo", not in the keys; not locked.
    runScript(script, fakeWindow('/enfoo/01-something'));
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });
});
