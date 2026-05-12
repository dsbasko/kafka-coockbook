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

const { ReadingProgress } = await import('./ReadingProgress');
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

function renderBar() {
  act(() => {
    root.render(<ReadingProgress />);
  });
}

describe('ReadingProgress', () => {
  it('uses the EN reading-progress aria-label when lang=en', () => {
    renderBar();
    const bar = container.querySelector('[role="progressbar"]');
    expect(bar?.getAttribute('aria-label')).toBe(UI_STRINGS.en.readingProgressLabel);
  });

  it('uses the RU reading-progress aria-label when lang=ru', () => {
    paramsRef.current = { lang: 'ru' };
    renderBar();
    const bar = container.querySelector('[role="progressbar"]');
    expect(bar?.getAttribute('aria-label')).toBe(UI_STRINGS.ru.readingProgressLabel);
  });
});
