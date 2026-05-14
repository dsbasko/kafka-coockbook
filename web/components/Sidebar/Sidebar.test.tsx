import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

const paramsRef: { current: Record<string, string | string[] | undefined> | null } = {
  current: { lang: 'en' },
};
const pathnameRef: { current: string | null } = { current: '/en/' };

vi.mock('next/navigation', () => ({
  useParams: () => paramsRef.current,
  usePathname: () => pathnameRef.current,
}));

vi.mock('@/components/ThemeToggle', () => ({
  ThemeToggle: () => <div data-testid="theme-toggle-stub" />,
}));

vi.mock('@/components/LanguageToggle', () => ({
  LanguageToggle: () => <div data-testid="language-toggle-stub" />,
}));

vi.mock('@/components/ReadingPrefsToggle', () => ({
  ReadingPrefsToggle: () => <div data-testid="reading-prefs-toggle-stub" />,
}));

const { Sidebar } = await import('./Sidebar');
const { UI_STRINGS } = await import('@/lib/i18n');

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  paramsRef.current = { lang: 'en' };
  pathnameRef.current = '/en/';
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

function renderSidebar() {
  act(() => {
    root.render(
      <Sidebar
        onProgramClick={() => {}}
        isProgramOpen={false}
        repoUrl="https://example.com/repo"
      />,
    );
  });
}

function ariaLabels(): string[] {
  return Array.from(container.querySelectorAll<HTMLElement>('[aria-label]')).map(
    (el) => el.getAttribute('aria-label') ?? '',
  );
}

describe('Sidebar', () => {
  it('uses EN aria-labels when lang=en', () => {
    renderSidebar();
    const labels = ariaLabels();
    expect(labels).toContain(UI_STRINGS.en.sidebarLabel);
    expect(labels).toContain(UI_STRINGS.en.navMainLabel);
    expect(labels).toContain(UI_STRINGS.en.home);
    expect(labels).toContain(UI_STRINGS.en.programCourse);
    expect(labels).toContain(UI_STRINGS.en.githubRepo);
  });

  it('uses RU aria-labels when lang=ru', () => {
    paramsRef.current = { lang: 'ru' };
    pathnameRef.current = '/ru/';
    renderSidebar();
    const labels = ariaLabels();
    expect(labels).toContain(UI_STRINGS.ru.sidebarLabel);
    expect(labels).toContain(UI_STRINGS.ru.navMainLabel);
    expect(labels).toContain(UI_STRINGS.ru.home);
    expect(labels).toContain(UI_STRINGS.ru.programCourse);
    expect(labels).toContain(UI_STRINGS.ru.githubRepo);
  });

  it('mirrors the home aria-label into title and renders the repo link with the repoUrl', () => {
    renderSidebar();
    const home = container.querySelector<HTMLAnchorElement>(`a[aria-label="${UI_STRINGS.en.home}"]`);
    expect(home?.getAttribute('title')).toBe(UI_STRINGS.en.home);
    const repo = container.querySelector<HTMLAnchorElement>(`a[aria-label="${UI_STRINGS.en.githubRepo}"]`);
    expect(repo?.getAttribute('href')).toBe('https://example.com/repo');
  });

  it('renders LanguageToggle, ReadingPrefsToggle, and ThemeToggle in order in the footer', () => {
    renderSidebar();
    const language = container.querySelector('[data-testid="language-toggle-stub"]');
    const readingPrefs = container.querySelector('[data-testid="reading-prefs-toggle-stub"]');
    const theme = container.querySelector('[data-testid="theme-toggle-stub"]');
    expect(language).not.toBeNull();
    expect(readingPrefs).not.toBeNull();
    expect(theme).not.toBeNull();
    expect(
      language!.compareDocumentPosition(readingPrefs!) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(
      readingPrefs!.compareDocumentPosition(theme!) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });
});
