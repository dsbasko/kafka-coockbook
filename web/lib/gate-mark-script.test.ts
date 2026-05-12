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

function makeCoursesByLang(): Record<'ru' | 'en', Course> {
  return { ru: parseCourse(COURSE_YAML, 'ru'), en: parseCourse(COURSE_YAML, 'en') };
}

type FakeWindow = { localStorage: Storage; location: { pathname: string } };

function runScript(script: string, fakeWindow: FakeWindow): void {
  const fn = new Function('window', script);
  fn(fakeWindow);
}

function makeFakeWindow(pathname = '/'): FakeWindow {
  return { localStorage: window.localStorage, location: { pathname } };
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
    const script = buildGateMarkScript(makeCoursesByLang(), '', 'en');
    runScript(script, makeFakeWindow('/en/'));
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
    const script = buildGateMarkScript(makeCoursesByLang(), '', 'en');
    runScript(script, makeFakeWindow('/en/02-bar/02-01-start/'));
    expect(row01.hasAttribute('data-locked')).toBe(false);
    expect(row02.hasAttribute('data-locked')).toBe(false);
    expect(row03.hasAttribute('data-locked')).toBe(false);
    expect(row04.hasAttribute('data-locked')).toBe(false);
  });

  it('builds CTA hrefs with the active lang prefix derived from URL', () => {
    document.body.innerHTML = '';
    const cta = document.createElement('div');
    cta.setAttribute('data-cta-frontier', 'global');
    const link = document.createElement('a');
    link.setAttribute('data-cta-frontier-link', '');
    link.setAttribute('href', '#');
    cta.appendChild(link);
    document.body.appendChild(cta);

    const script = buildGateMarkScript(makeCoursesByLang(), '/test', 'en');
    runScript(script, makeFakeWindow('/test/ru/01-foo/01-01-intro/'));
    expect(link.getAttribute('href')).toBe('/test/ru/01-foo/01-01-intro/');
  });

  it('does not strip basePath when only a substring prefix matches', () => {
    // basePath '/foo' must not match '/foobar/...': segment-aware check
    // mirrors LessonAwareLink so the lang strip downstream still sees the
    // original path.
    document.body.innerHTML = '';
    const cta = document.createElement('div');
    cta.setAttribute('data-cta-frontier', 'global');
    const link = document.createElement('a');
    link.setAttribute('data-cta-frontier-link', '');
    link.setAttribute('href', '#');
    cta.appendChild(link);
    document.body.appendChild(cta);

    const script = buildGateMarkScript(makeCoursesByLang(), '/foo', 'en');
    runScript(script, makeFakeWindow('/foobar/ru/01-foo/01-01-intro/'));
    // basePath should NOT be stripped from '/foobar/...'; lang regex then
    // also fails to match (path doesn't start with /ru/ or /en/), so the
    // CTA falls back to defaultLang ('en').
    expect(link.getAttribute('href')).toBe('/foo/en/01-foo/01-01-intro/');
  });

  it('writes lang-matching titles into CTA slots so RU pages don\'t flash EN titles', () => {
    // Reuses the test fixture (titles identical across langs without a {ru,en}
    // map in YAML), but exercises the lookup path. The same call is also used
    // by the production layout via lang-specific course objects.
    document.body.innerHTML = '';
    const cta = document.createElement('div');
    cta.setAttribute('data-cta-frontier', 'global');
    const titleEl = document.createElement('span');
    titleEl.setAttribute('data-cta-frontier-title', '');
    cta.appendChild(titleEl);
    document.body.appendChild(cta);

    const ruCourse = parseCourse(COURSE_YAML, 'ru');
    const enCourse = parseCourse(COURSE_YAML, 'en');
    // Mutate the first lesson title to differ between langs so we can verify
    // the script picks by lang prefix from the URL.
    ruCourse.modules[0].lessons[0].title = 'РУ Заголовок';
    enCourse.modules[0].lessons[0].title = 'EN Title';
    const script = buildGateMarkScript({ ru: ruCourse, en: enCourse }, '', 'en');
    runScript(script, makeFakeWindow('/ru/'));
    expect(titleEl.textContent).toBe('РУ Заголовок');
  });
});

describe('applyGatePainting', () => {
  it('paints data-locked on future rows from the imperative API', () => {
    const { row01, row02, row03, row04 } = mountLessonRows();
    applyGatePainting(makeCourse(), -1, '', 'en');
    expect(row01.hasAttribute('data-locked')).toBe(false);
    expect(row02.getAttribute('data-locked')).toBe('true');
    expect(row03.getAttribute('data-locked')).toBe('true');
    expect(row04.getAttribute('data-locked')).toBe('true');
  });

  it('writes lang-prefixed CTA hrefs', () => {
    const cta = document.createElement('div');
    cta.setAttribute('data-cta-frontier', 'global');
    const link = document.createElement('a');
    link.setAttribute('data-cta-frontier-link', '');
    link.setAttribute('href', '#');
    cta.appendChild(link);
    document.body.appendChild(cta);

    applyGatePainting(makeCourse(), -1, '/test', 'ru');
    expect(link.getAttribute('href')).toBe('/test/ru/01-foo/01-01-intro/');
  });
});
