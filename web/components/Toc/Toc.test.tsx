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

const { Toc } = await import('./Toc');
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

function renderToc() {
  act(() => {
    root.render(
      <Toc
        entries={[
          { slug: 'intro', depth: 2, text: 'Intro' },
          { slug: 'body', depth: 2, text: 'Body' },
        ]}
      />,
    );
  });
}

describe('Toc', () => {
  it('uses the EN tocLabel for the nav aria-label when lang=en', () => {
    renderToc();
    const nav = container.querySelector('nav');
    expect(nav?.getAttribute('aria-label')).toBe(UI_STRINGS.en.tocLabel);
  });

  it('uses the RU tocLabel for the nav aria-label when lang=ru', () => {
    paramsRef.current = { lang: 'ru' };
    renderToc();
    const nav = container.querySelector('nav');
    expect(nav?.getAttribute('aria-label')).toBe(UI_STRINGS.ru.tocLabel);
  });

  it('renders entry labels under their slug anchors', () => {
    renderToc();
    const links = Array.from(container.querySelectorAll<HTMLAnchorElement>('a'));
    expect(links.map((a) => a.textContent?.trim())).toEqual(['Intro', 'Body']);
    expect(links.map((a) => a.getAttribute('href'))).toEqual(['#intro', '#body']);
  });
});
