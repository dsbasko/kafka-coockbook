export type SizeStep = 0 | 1 | 2 | 3 | 4;
export type ProseFont = 'serif' | 'sans' | 'lora';
export type CodeFont = 'jetbrains' | 'fira' | 'plex';

export interface ReadingPrefs {
  proseSize: SizeStep;
  codeSize: SizeStep;
  proseFont: ProseFont;
  codeFont: CodeFont;
}

export const READING_PREFS_STORAGE_KEY = 'kafka-cookbook-reading-prefs:v1';

export const PROSE_FONTS = ['serif', 'sans', 'lora'] as const;
export const CODE_FONTS = ['jetbrains', 'fira', 'plex'] as const;
export const SIZE_STEPS = [0, 1, 2, 3, 4] as const;

export const DEFAULT_PREFS: ReadingPrefs = {
  proseSize: 2,
  codeSize: 2,
  proseFont: 'serif',
  codeFont: 'jetbrains',
};

export function isProseFont(value: unknown): value is ProseFont {
  return value === 'serif' || value === 'sans' || value === 'lora';
}

export function isCodeFont(value: unknown): value is CodeFont {
  return value === 'jetbrains' || value === 'fira' || value === 'plex';
}

export function isSizeStep(value: unknown): value is SizeStep {
  return value === 0 || value === 1 || value === 2 || value === 3 || value === 4;
}

export function clampSize(value: unknown): SizeStep {
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n)) return DEFAULT_PREFS.proseSize;
  const rounded = Math.round(n);
  if (rounded <= 0) return 0;
  if (rounded >= 4) return 4;
  return rounded as SizeStep;
}

function validateField<T>(value: unknown, guard: (v: unknown) => v is T, fallback: T): T {
  return guard(value) ? value : fallback;
}

export function readStoredPrefs(): ReadingPrefs {
  if (typeof window === 'undefined') return { ...DEFAULT_PREFS };
  let raw: string | null = null;
  try {
    raw = window.localStorage.getItem(READING_PREFS_STORAGE_KEY);
  } catch {
    return { ...DEFAULT_PREFS };
  }
  if (raw == null) return { ...DEFAULT_PREFS };
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ...DEFAULT_PREFS };
  }
  if (!parsed || typeof parsed !== 'object') return { ...DEFAULT_PREFS };
  const obj = parsed as Record<string, unknown>;
  return {
    proseSize: validateField(obj.proseSize, isSizeStep, DEFAULT_PREFS.proseSize),
    codeSize: validateField(obj.codeSize, isSizeStep, DEFAULT_PREFS.codeSize),
    proseFont: validateField(obj.proseFont, isProseFont, DEFAULT_PREFS.proseFont),
    codeFont: validateField(obj.codeFont, isCodeFont, DEFAULT_PREFS.codeFont),
  };
}

export function writeStoredPrefs(prefs: ReadingPrefs): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(READING_PREFS_STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    /* storage may be unavailable (private mode, quota); ignore. */
  }
}

export function applyPrefs(prefs: ReadingPrefs): void {
  if (typeof document === 'undefined') return;
  const ds = document.documentElement.dataset;
  ds.proseSize = String(prefs.proseSize);
  ds.codeSize = String(prefs.codeSize);
  ds.proseFont = prefs.proseFont;
  ds.codeFont = prefs.codeFont;
}

/**
 * Inline script injected into <head> before hydration. Reads localStorage,
 * validates per-field, and stamps four data-* attributes on <html>
 * synchronously to avoid FOUC.
 */
export const READING_PREFS_INIT_SCRIPT = `(() => {
  var KEY = ${JSON.stringify(READING_PREFS_STORAGE_KEY)};
  var defaults = ${JSON.stringify(DEFAULT_PREFS)};
  var sizes = [0, 1, 2, 3, 4];
  var proseFonts = ['serif', 'sans', 'lora'];
  var codeFonts = ['jetbrains', 'fira', 'plex'];
  var prefs = {
    proseSize: defaults.proseSize,
    codeSize: defaults.codeSize,
    proseFont: defaults.proseFont,
    codeFont: defaults.codeFont,
  };
  try {
    var raw = null;
    try { raw = window.localStorage.getItem(KEY); } catch (_) {}
    if (raw != null) {
      var parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        if (sizes.indexOf(parsed.proseSize) !== -1) prefs.proseSize = parsed.proseSize;
        if (sizes.indexOf(parsed.codeSize) !== -1) prefs.codeSize = parsed.codeSize;
        if (proseFonts.indexOf(parsed.proseFont) !== -1) prefs.proseFont = parsed.proseFont;
        if (codeFonts.indexOf(parsed.codeFont) !== -1) prefs.codeFont = parsed.codeFont;
      }
    }
  } catch (_) {}
  try {
    var ds = document.documentElement.dataset;
    ds.proseSize = String(prefs.proseSize);
    ds.codeSize = String(prefs.codeSize);
    ds.proseFont = prefs.proseFont;
    ds.codeFont = prefs.codeFont;
  } catch (_) {}
})();`;
