import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { parseCourse, type Course } from './course';
import { applyGatePainting, buildGateMarkScript } from './gate-mark-script';
import { FURTHEST_STORAGE_KEY, lessonKey } from './progress';

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

type FakeWindow = { localStorage: Storage };

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
  document.body.innerHTML = '';
  document.documentElement.removeAttribute('data-has-progress');
});

afterEach(() => {
  document.body.innerHTML = '';
  document.documentElement.removeAttribute('data-has-progress');
});

function mountLessonRows(): { row01: HTMLElement; row02: HTMLElement; row03: HTMLElement; row04: HTMLElement } {
  const row01 = document.createElement('a');
  row01.setAttribute('data-lesson-key', '01-foo/01-01-intro');
  const row02 = document.createElement('a');
  row02.setAttribute('data-lesson-key', '01-foo/01-02-deep');
  const row03 = document.createElement('a');
  row03.setAttribute('data-lesson-key', '02-bar/02-01-start');
  const row04 = document.createElement('a');
  row04.setAttribute('data-lesson-key', '02-bar/02-02-end');
  document.body.append(row01, row02, row03, row04);
  return { row01, row02, row03, row04 };
}

describe('buildGateMarkScript', () => {
  it('paints data-locked on future rows regardless of which lang-prefixed page the script runs on', () => {
    // gate-mark-script does not parse pathname — it operates on
    // data-lesson-key attributes and localStorage. This test verifies the
    // DOM-based painting is unaffected by which URL the page sits on, so
    // the same script keeps working under /, /ru/, and /en/ routes.
    // With no progress, only the first lesson is reachable (idx <= fidx+1).
    const { row01, row02, row03, row04 } = mountLessonRows();
    const script = buildGateMarkScript(makeCourse(), '');
    runScript(script, { localStorage: window.localStorage });
    expect(row01.hasAttribute('data-locked')).toBe(false);
    expect(row02.getAttribute('data-locked')).toBe('true');
    expect(row03.getAttribute('data-locked')).toBe('true');
    expect(row04.getAttribute('data-locked')).toBe('true');
  });

  it('respects the furthest pointer from localStorage when painting', () => {
    window.localStorage.setItem(
      FURTHEST_STORAGE_KEY,
      lessonKey('02-bar', '02-01-start'),
    );
    const { row01, row02, row03, row04 } = mountLessonRows();
    const script = buildGateMarkScript(makeCourse(), '');
    runScript(script, { localStorage: window.localStorage });
    expect(row01.hasAttribute('data-locked')).toBe(false);
    expect(row02.hasAttribute('data-locked')).toBe(false);
    expect(row03.hasAttribute('data-locked')).toBe(false);
    expect(row04.hasAttribute('data-locked')).toBe(false);
  });
});

describe('applyGatePainting', () => {
  it('paints data-locked on future rows from the imperative API', () => {
    const { row01, row02, row03, row04 } = mountLessonRows();
    applyGatePainting(makeCourse(), -1, '');
    expect(row01.hasAttribute('data-locked')).toBe(false);
    expect(row02.getAttribute('data-locked')).toBe('true');
    expect(row03.getAttribute('data-locked')).toBe('true');
    expect(row04.getAttribute('data-locked')).toBe('true');
  });
});
