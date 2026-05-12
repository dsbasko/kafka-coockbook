import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

const paramsRef: { current: Record<string, string | string[] | undefined> | null } = {
  current: { lang: 'en' },
};

vi.mock('next/navigation', () => ({
  useParams: () => paramsRef.current,
}));

vi.mock('@/components/GateProvider', () => ({
  useGate: () => ({
    course: { modules: [] },
    progress: null,
    hydrated: false,
    furthestIndex: 0,
    basePath: '',
  }),
}));

vi.mock('@/lib/progress', () => ({
  lessonKey: (a: string, b: string) => `${a}/${b}`,
  markCompletedAndAdvance: vi.fn(),
}));

const { LessonNav } = await import('./LessonNav');
const { UI_STRINGS } = await import('@/lib/i18n');

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  paramsRef.current = { lang: 'en' };
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(() => {
  act(() => {
    root.unmount();
  });
  container.remove();
});

const PREV = { moduleId: '01-foundations', slug: '01-01-intro', title: 'Intro' };
const NEXT = { moduleId: '01-foundations', slug: '01-03-deep', title: 'Deep dive' };

function renderNav(prev: typeof PREV | null, next: typeof NEXT | null) {
  act(() => {
    root.render(
      <LessonNav
        prev={prev}
        next={next}
        currentModuleId="01-foundations"
        currentSlug="01-02-mid"
      />,
    );
  });
}

describe('LessonNav', () => {
  it('uses the EN nav aria-label and prev/next labels when lang=en', () => {
    renderNav(PREV, NEXT);
    const nav = container.querySelector('nav');
    expect(nav?.getAttribute('aria-label')).toBe(UI_STRINGS.en.lessonNavLabel);
    expect(container.textContent).toContain(UI_STRINGS.en.prevLesson);
    expect(container.textContent).toContain(UI_STRINGS.en.nextLesson);
    expect(container.textContent).toContain('Intro');
    expect(container.textContent).toContain('Deep dive');
  });

  it('uses RU strings when lang=ru', () => {
    paramsRef.current = { lang: 'ru' };
    renderNav(PREV, NEXT);
    const nav = container.querySelector('nav');
    expect(nav?.getAttribute('aria-label')).toBe(UI_STRINGS.ru.lessonNavLabel);
    expect(container.textContent).toContain(UI_STRINGS.ru.prevLesson);
    expect(container.textContent).toContain(UI_STRINGS.ru.nextLesson);
  });

  it('renders placeholders instead of links when prev/next are null', () => {
    renderNav(null, null);
    expect(container.querySelectorAll('a')).toHaveLength(0);
    expect(container.querySelectorAll('[aria-hidden="true"]')).toHaveLength(2);
  });
});
