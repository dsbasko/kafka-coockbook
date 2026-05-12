import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_LANG,
  LANGS,
  LANG_INIT_SCRIPT,
  LANG_LABELS,
  LANG_STORAGE_KEY,
  LANG_SYNC_SCRIPT,
  getBrowserLang,
  isLang,
  readStoredLang,
  stripLangFromPath,
  writeStoredLang,
} from './lang';

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

function mockNavigatorLanguage(language: string) {
  Object.defineProperty(window.navigator, 'language', {
    configurable: true,
    get: () => language,
  });
  Object.defineProperty(window.navigator, 'languages', {
    configurable: true,
    get: () => [language],
  });
}

describe('constants', () => {
  it('LANGS contains ru and en', () => {
    expect([...LANGS]).toEqual(['ru', 'en']);
  });

  it('DEFAULT_LANG is en', () => {
    expect(DEFAULT_LANG).toBe('en');
  });

  it('LANG_STORAGE_KEY matches the kafka-cookbook namespace', () => {
    expect(LANG_STORAGE_KEY).toBe('kafka-cookbook-lang');
  });

  it('LANG_LABELS covers every language', () => {
    expect(LANG_LABELS.ru).toBe('Русский');
    expect(LANG_LABELS.en).toBe('English');
  });
});

describe('isLang', () => {
  it('accepts ru and en', () => {
    expect(isLang('ru')).toBe(true);
    expect(isLang('en')).toBe(true);
  });

  it('rejects everything else', () => {
    expect(isLang('')).toBe(false);
    expect(isLang(null)).toBe(false);
    expect(isLang(undefined)).toBe(false);
    expect(isLang('RU')).toBe(false);
    expect(isLang('fr')).toBe(false);
    expect(isLang(42)).toBe(false);
  });
});

describe('getBrowserLang', () => {
  it('returns ru when navigator.language starts with ru', () => {
    mockNavigatorLanguage('ru-RU');
    expect(getBrowserLang()).toBe('ru');
  });

  it('returns en when navigator.language starts with en', () => {
    mockNavigatorLanguage('en-US');
    expect(getBrowserLang()).toBe('en');
  });

  it('returns en for unknown languages', () => {
    mockNavigatorLanguage('fr-FR');
    expect(getBrowserLang()).toBe('en');
  });

  it('is case-insensitive on the language prefix', () => {
    mockNavigatorLanguage('RU-ru');
    expect(getBrowserLang()).toBe('ru');
  });
});

describe('readStoredLang / writeStoredLang', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it('returns null when nothing is stored', () => {
    expect(readStoredLang()).toBeNull();
  });

  it('returns null when stored value is invalid', () => {
    window.localStorage.setItem(LANG_STORAGE_KEY, 'fr');
    expect(readStoredLang()).toBeNull();
  });

  it('round-trips a valid language', () => {
    writeStoredLang('ru');
    expect(window.localStorage.getItem(LANG_STORAGE_KEY)).toBe('ru');
    expect(readStoredLang()).toBe('ru');

    writeStoredLang('en');
    expect(readStoredLang()).toBe('en');
  });
});

describe('stripLangFromPath', () => {
  it('extracts ru prefix and returns the rest', () => {
    expect(stripLangFromPath('/ru/foo/bar')).toEqual({ lang: 'ru', rest: '/foo/bar' });
  });

  it('extracts en prefix and returns the rest', () => {
    expect(stripLangFromPath('/en/01-foundations/01-01-architecture-and-kraft')).toEqual({
      lang: 'en',
      rest: '/01-foundations/01-01-architecture-and-kraft',
    });
  });

  it('handles trailing slash on the lang segment', () => {
    expect(stripLangFromPath('/ru/')).toEqual({ lang: 'ru', rest: '/' });
  });

  it('handles a bare lang segment without trailing slash', () => {
    expect(stripLangFromPath('/ru')).toEqual({ lang: 'ru', rest: '/' });
    expect(stripLangFromPath('/en')).toEqual({ lang: 'en', rest: '/' });
  });

  it('returns null lang for paths without a lang prefix', () => {
    expect(stripLangFromPath('/foo/bar')).toEqual({ lang: null, rest: '/foo/bar' });
    expect(stripLangFromPath('/')).toEqual({ lang: null, rest: '/' });
  });

  it('does not match partial prefixes like /enfoo or /russian', () => {
    expect(stripLangFromPath('/enfoo')).toEqual({ lang: null, rest: '/enfoo' });
    expect(stripLangFromPath('/russian/docs')).toEqual({ lang: null, rest: '/russian/docs' });
  });

  it('returns root rest for an empty pathname', () => {
    expect(stripLangFromPath('')).toEqual({ lang: null, rest: '/' });
  });
});

describe('LANG_SYNC_SCRIPT', () => {
  afterEach(() => {
    document.documentElement.removeAttribute('lang');
  });

  it('sets document.documentElement.lang to the given language', () => {
    new Function(LANG_SYNC_SCRIPT('ru'))();
    expect(document.documentElement.lang).toBe('ru');

    new Function(LANG_SYNC_SCRIPT('en'))();
    expect(document.documentElement.lang).toBe('en');
  });
});

type LocationMock = {
  pathname: string;
  replace: ReturnType<typeof vi.fn>;
};

describe('LANG_INIT_SCRIPT', () => {
  let location: LocationMock;
  let runScript: () => void;

  beforeEach(() => {
    window.localStorage.clear();
    location = {
      pathname: '/',
      replace: vi.fn(),
    };
    runScript = () => {
      const fn = new Function('navigator', 'window', LANG_INIT_SCRIPT);
      const fakeWindow = {
        localStorage: window.localStorage,
        location,
      };
      fn(window.navigator, fakeWindow);
    };
  });

  it('skips redirect when stored language equals DEFAULT_LANG', () => {
    window.localStorage.setItem(LANG_STORAGE_KEY, 'en');
    runScript();
    expect(location.replace).not.toHaveBeenCalled();
  });

  it('redirects to /ru/ when stored language is ru regardless of navigator', () => {
    window.localStorage.setItem(LANG_STORAGE_KEY, 'ru');
    mockNavigatorLanguage('en-US');
    runScript();
    expect(location.replace).toHaveBeenCalledWith('/ru/');
  });

  it('auto-detects ru from navigator and persists it before redirecting', () => {
    mockNavigatorLanguage('ru-RU');
    runScript();
    expect(window.localStorage.getItem(LANG_STORAGE_KEY)).toBe('ru');
    expect(location.replace).toHaveBeenCalledWith('/ru/');
  });

  it('auto-detects en from navigator, persists, and skips redirect (already on en)', () => {
    mockNavigatorLanguage('en-US');
    runScript();
    expect(window.localStorage.getItem(LANG_STORAGE_KEY)).toBe('en');
    expect(location.replace).not.toHaveBeenCalled();
  });

  it('respects an existing basePath in the pathname when redirecting', () => {
    location.pathname = '/kafka-cookbook/';
    mockNavigatorLanguage('ru-RU');
    runScript();
    expect(location.replace).toHaveBeenCalledWith('/kafka-cookbook/ru/');
  });

  it('falls back to en for unknown navigator languages', () => {
    mockNavigatorLanguage('fr-FR');
    runScript();
    expect(window.localStorage.getItem(LANG_STORAGE_KEY)).toBe('en');
    expect(location.replace).not.toHaveBeenCalled();
  });
});
