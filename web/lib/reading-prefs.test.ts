import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  applyPrefs,
  clampSize,
  DEFAULT_PREFS,
  isCodeFont,
  isProseFont,
  isSizeStep,
  READING_PREFS_INIT_SCRIPT,
  READING_PREFS_STORAGE_KEY,
  readStoredPrefs,
  writeStoredPrefs,
} from './reading-prefs';

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

function clearHtmlAttrs() {
  const el = document.documentElement;
  el.removeAttribute('data-prose-size');
  el.removeAttribute('data-code-size');
  el.removeAttribute('data-prose-font');
  el.removeAttribute('data-code-font');
}

describe('isProseFont', () => {
  it('accepts serif, sans, lora', () => {
    expect(isProseFont('serif')).toBe(true);
    expect(isProseFont('sans')).toBe(true);
    expect(isProseFont('lora')).toBe(true);
  });

  it('rejects everything else', () => {
    expect(isProseFont('SERIF')).toBe(false);
    expect(isProseFont('')).toBe(false);
    expect(isProseFont(null)).toBe(false);
    expect(isProseFont(undefined)).toBe(false);
    expect(isProseFont(42)).toBe(false);
  });
});

describe('isCodeFont', () => {
  it('accepts jetbrains, fira, plex', () => {
    expect(isCodeFont('jetbrains')).toBe(true);
    expect(isCodeFont('fira')).toBe(true);
    expect(isCodeFont('plex')).toBe(true);
  });

  it('rejects everything else', () => {
    expect(isCodeFont('mono')).toBe(false);
    expect(isCodeFont(null)).toBe(false);
    expect(isCodeFont(3)).toBe(false);
  });
});

describe('isSizeStep', () => {
  it('accepts 0..4', () => {
    expect(isSizeStep(0)).toBe(true);
    expect(isSizeStep(1)).toBe(true);
    expect(isSizeStep(2)).toBe(true);
    expect(isSizeStep(3)).toBe(true);
    expect(isSizeStep(4)).toBe(true);
  });

  it('rejects out of range and non-integers', () => {
    expect(isSizeStep(-1)).toBe(false);
    expect(isSizeStep(5)).toBe(false);
    expect(isSizeStep(1.5)).toBe(false);
    expect(isSizeStep('2')).toBe(false);
    expect(isSizeStep(null)).toBe(false);
  });
});

describe('clampSize', () => {
  it('returns valid step unchanged', () => {
    expect(clampSize(0)).toBe(0);
    expect(clampSize(2)).toBe(2);
    expect(clampSize(4)).toBe(4);
  });

  it('clamps below to 0', () => {
    expect(clampSize(-5)).toBe(0);
    expect(clampSize(-1)).toBe(0);
  });

  it('clamps above to 4', () => {
    expect(clampSize(5)).toBe(4);
    expect(clampSize(100)).toBe(4);
  });

  it('rounds floats', () => {
    expect(clampSize(1.4)).toBe(1);
    expect(clampSize(1.6)).toBe(2);
  });

  it('falls back for non-finite values', () => {
    expect(clampSize(NaN)).toBe(DEFAULT_PREFS.proseSize);
    expect(clampSize(Infinity)).toBe(DEFAULT_PREFS.proseSize);
    expect(clampSize('abc')).toBe(DEFAULT_PREFS.proseSize);
  });

  it('parses numeric string', () => {
    expect(clampSize('3')).toBe(3);
  });
});

describe('readStoredPrefs / writeStoredPrefs', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it('returns defaults when nothing is stored', () => {
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
  });

  it('returns defaults when JSON is malformed', () => {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, 'not-json{');
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
  });

  it('returns defaults when stored value is not an object', () => {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, '"oops"');
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, 'null');
    expect(readStoredPrefs()).toEqual(DEFAULT_PREFS);
  });

  it('keeps valid fields and substitutes defaults for invalid ones', () => {
    window.localStorage.setItem(
      READING_PREFS_STORAGE_KEY,
      JSON.stringify({
        proseSize: 4,
        codeSize: 99,
        proseFont: 'lora',
        codeFont: 'unknown',
      }),
    );
    expect(readStoredPrefs()).toEqual({
      proseSize: 4,
      codeSize: DEFAULT_PREFS.codeSize,
      proseFont: 'lora',
      codeFont: DEFAULT_PREFS.codeFont,
    });
  });

  it('round-trips a full prefs object', () => {
    const prefs = { proseSize: 3, codeSize: 1, proseFont: 'sans', codeFont: 'fira' } as const;
    writeStoredPrefs(prefs);
    expect(window.localStorage.getItem(READING_PREFS_STORAGE_KEY)).toBe(JSON.stringify(prefs));
    expect(readStoredPrefs()).toEqual(prefs);
  });
});

describe('applyPrefs', () => {
  afterEach(() => {
    clearHtmlAttrs();
  });

  it('writes all four data-* attributes', () => {
    applyPrefs({ proseSize: 3, codeSize: 4, proseFont: 'sans', codeFont: 'plex' });
    expect(document.documentElement.dataset.proseSize).toBe('3');
    expect(document.documentElement.dataset.codeSize).toBe('4');
    expect(document.documentElement.dataset.proseFont).toBe('sans');
    expect(document.documentElement.dataset.codeFont).toBe('plex');
  });
});

describe('READING_PREFS_INIT_SCRIPT', () => {
  beforeEach(() => {
    window.localStorage.clear();
    clearHtmlAttrs();
  });

  afterEach(() => {
    clearHtmlAttrs();
  });

  it('writes defaults when nothing is stored', () => {
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe(String(DEFAULT_PREFS.proseSize));
    expect(document.documentElement.dataset.codeSize).toBe(String(DEFAULT_PREFS.codeSize));
    expect(document.documentElement.dataset.proseFont).toBe(DEFAULT_PREFS.proseFont);
    expect(document.documentElement.dataset.codeFont).toBe(DEFAULT_PREFS.codeFont);
  });

  it('uses stored values when fully valid', () => {
    window.localStorage.setItem(
      READING_PREFS_STORAGE_KEY,
      JSON.stringify({ proseSize: 4, codeSize: 1, proseFont: 'lora', codeFont: 'plex' }),
    );
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe('4');
    expect(document.documentElement.dataset.codeSize).toBe('1');
    expect(document.documentElement.dataset.proseFont).toBe('lora');
    expect(document.documentElement.dataset.codeFont).toBe('plex');
  });

  it('falls back to defaults when JSON is malformed', () => {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, 'broken{json');
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe(String(DEFAULT_PREFS.proseSize));
    expect(document.documentElement.dataset.codeSize).toBe(String(DEFAULT_PREFS.codeSize));
    expect(document.documentElement.dataset.proseFont).toBe(DEFAULT_PREFS.proseFont);
    expect(document.documentElement.dataset.codeFont).toBe(DEFAULT_PREFS.codeFont);
  });

  it('partially valid object keeps good fields, defaults the rest', () => {
    window.localStorage.setItem(
      READING_PREFS_STORAGE_KEY,
      JSON.stringify({ proseSize: 3, codeSize: 'huge', proseFont: 'sans', codeFont: 9 }),
    );
    new Function(READING_PREFS_INIT_SCRIPT)();
    expect(document.documentElement.dataset.proseSize).toBe('3');
    expect(document.documentElement.dataset.codeSize).toBe(String(DEFAULT_PREFS.codeSize));
    expect(document.documentElement.dataset.proseFont).toBe('sans');
    expect(document.documentElement.dataset.codeFont).toBe(DEFAULT_PREFS.codeFont);
  });
});
