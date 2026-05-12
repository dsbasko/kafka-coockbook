import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { GATE_LOCKED_ATTR } from '@/lib/gate-init-script';
import { parseCourse, type Course } from '@/lib/course';
import { lessonKey } from '@/lib/progress';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

const pathnameRef: { current: string | null } = { current: '/' };

vi.mock('next/navigation', () => ({
  usePathname: () => pathnameRef.current,
}));

const { GateProvider } = await import('./GateProvider');

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

function loadCourse(): Course {
  return parseCourse(COURSE_YAML);
}

let container: HTMLDivElement;
let root: Root;

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
  pathnameRef.current = '/';
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  document.documentElement.removeAttribute(GATE_LOCKED_ATTR);
});

afterEach(() => {
  act(() => {
    root.unmount();
  });
  container.remove();
  document.documentElement.removeAttribute(GATE_LOCKED_ATTR);
});

function renderGate(course: Course) {
  act(() => {
    root.render(
      <GateProvider course={course} basePath="">
        <div />
      </GateProvider>,
    );
  });
}

describe('GateProvider gate-locked attribute', () => {
  it('does not lock on the home pathname', () => {
    pathnameRef.current = '/';
    renderGate(loadCourse());
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('does not lock for the first reachable lesson at /moduleId/slug', () => {
    pathnameRef.current = '/01-foo/01-01-intro';
    renderGate(loadCourse());
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('locks a future lesson at /moduleId/slug', () => {
    pathnameRef.current = '/02-bar/02-02-end';
    renderGate(loadCourse());
    expect(document.documentElement.getAttribute(GATE_LOCKED_ATTR)).toBe('true');
  });

  it('locks a future lesson under /ru/ prefix', () => {
    pathnameRef.current = '/ru/02-bar/02-02-end';
    renderGate(loadCourse());
    expect(document.documentElement.getAttribute(GATE_LOCKED_ATTR)).toBe('true');
  });

  it('locks a future lesson under /en/ prefix', () => {
    pathnameRef.current = '/en/02-bar/02-02-end';
    renderGate(loadCourse());
    expect(document.documentElement.getAttribute(GATE_LOCKED_ATTR)).toBe('true');
  });

  it('does not lock the first lesson under /ru/ prefix', () => {
    pathnameRef.current = '/ru/01-foo/01-01-intro';
    renderGate(loadCourse());
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('does not lock when stripped pathname has fewer than 2 segments', () => {
    pathnameRef.current = '/ru/';
    renderGate(loadCourse());
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });

  it('respects furthest pointer for /en/ paths when localStorage marks progress', () => {
    window.localStorage.setItem(
      'kafka-cookbook-furthest',
      lessonKey('02-bar', '02-01-start'),
    );
    pathnameRef.current = '/en/02-bar/02-02-end';
    renderGate(loadCourse());
    expect(document.documentElement.hasAttribute(GATE_LOCKED_ATTR)).toBe(false);
  });
});
