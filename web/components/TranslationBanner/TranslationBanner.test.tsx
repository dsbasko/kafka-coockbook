import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { TranslationBanner } from './TranslationBanner';
import { UI_STRINGS } from '@/lib/i18n';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
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

function render(lang: 'ru' | 'en') {
  act(() => {
    root.render(<TranslationBanner lang={lang} />);
  });
  const node = container.querySelector<HTMLElement>('[data-translation-banner]');
  if (!node) throw new Error('TranslationBanner did not render');
  return node;
}

describe('TranslationBanner', () => {
  it('renders the EN dictionary title and body when lang=en', () => {
    const node = render('en');
    expect(node.textContent).toContain(UI_STRINGS.en.translationFallbackTitle);
    expect(node.textContent).toContain(UI_STRINGS.en.translationFallbackBody);
  });

  it('renders the RU dictionary title and body when lang=ru', () => {
    const node = render('ru');
    expect(node.textContent).toContain(UI_STRINGS.ru.translationFallbackTitle);
    expect(node.textContent).toContain(UI_STRINGS.ru.translationFallbackBody);
  });

  it('uses an aside element with role=note for assistive tech', () => {
    const node = render('en');
    expect(node.tagName).toBe('ASIDE');
    expect(node.getAttribute('role')).toBe('note');
  });

  it('labels the banner with the localized title for screen readers', () => {
    const node = render('en');
    expect(node.getAttribute('aria-label')).toBe(
      UI_STRINGS.en.translationFallbackTitle,
    );
  });

  it('marks the SVG icon as decorative (aria-hidden)', () => {
    const node = render('en');
    const svg = node.querySelector('svg');
    expect(svg).not.toBeNull();
    expect(svg!.getAttribute('aria-hidden')).toBe('true');
  });
});
