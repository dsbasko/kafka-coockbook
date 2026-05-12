import { afterEach, describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { UI_STRINGS } from './i18n';

// Use a mutable holder so each test can override what `useParams` returns
// before the component renders. The hook reads `lang` synchronously, so
// nothing more involved than this is needed.
const paramsRef: { current: Record<string, string | string[] | undefined> | null } = {
  current: null,
};

vi.mock('next/navigation', () => ({
  useParams: () => paramsRef.current,
}));

// Import AFTER vi.mock so use-i18n's `next/navigation` import resolves to
// the mock. Plain require-style dynamic import keeps the test deterministic
// across Vitest's module graph.
const { useLang, useT } = await import('./use-i18n');

function LangProbe() {
  const lang = useLang();
  return <span data-testid="lang">{lang}</span>;
}

function TitleProbe() {
  const t = useT();
  return <h1 data-testid="title">{t.notFoundTitle}</h1>;
}

afterEach(() => {
  paramsRef.current = null;
});

describe('useLang', () => {
  it('returns ru when params.lang is "ru"', () => {
    paramsRef.current = { lang: 'ru' };
    const html = renderToStaticMarkup(<LangProbe />);
    expect(html).toContain('>ru<');
  });

  it('returns en when params.lang is "en"', () => {
    paramsRef.current = { lang: 'en' };
    const html = renderToStaticMarkup(<LangProbe />);
    expect(html).toContain('>en<');
  });

  it('returns DEFAULT_LANG (en) when params is null', () => {
    paramsRef.current = null;
    const html = renderToStaticMarkup(<LangProbe />);
    expect(html).toContain('>en<');
  });

  it('returns DEFAULT_LANG (en) when params.lang is missing', () => {
    paramsRef.current = {};
    const html = renderToStaticMarkup(<LangProbe />);
    expect(html).toContain('>en<');
  });

  it('returns DEFAULT_LANG (en) when params.lang is an unsupported value', () => {
    paramsRef.current = { lang: 'fr' };
    const html = renderToStaticMarkup(<LangProbe />);
    expect(html).toContain('>en<');
  });

  it('reads the first entry when params.lang is an array', () => {
    paramsRef.current = { lang: ['ru', 'leftover'] };
    const html = renderToStaticMarkup(<LangProbe />);
    expect(html).toContain('>ru<');
  });
});

describe('useT', () => {
  it('returns the EN dictionary when lang resolves to en', () => {
    paramsRef.current = { lang: 'en' };
    const html = renderToStaticMarkup(<TitleProbe />);
    expect(html).toContain(UI_STRINGS.en.notFoundTitle);
  });

  it('returns the RU dictionary when lang resolves to ru', () => {
    paramsRef.current = { lang: 'ru' };
    const html = renderToStaticMarkup(<TitleProbe />);
    expect(html).toContain(UI_STRINGS.ru.notFoundTitle);
  });

  it('falls back to DEFAULT_LANG (EN) when params absent', () => {
    paramsRef.current = null;
    const html = renderToStaticMarkup(<TitleProbe />);
    expect(html).toContain(UI_STRINGS.en.notFoundTitle);
  });
});
