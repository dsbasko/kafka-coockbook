export type ThemePreference = 'light' | 'dark' | 'system';
export type ResolvedTheme = 'light' | 'dark';

export const THEME_STORAGE_KEY = 'kafka-cookbook-theme';
export const THEME_PREFERENCES = ['light', 'dark', 'system'] as const;

export const THEME_LABELS: Record<ThemePreference, string> = {
  light: 'Светлая',
  dark: 'Тёмная',
  system: 'Системная',
};

export function isThemePreference(value: unknown): value is ThemePreference {
  return value === 'light' || value === 'dark' || value === 'system';
}

export function getSystemTheme(): ResolvedTheme {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return 'light';
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export function resolveTheme(preference: ThemePreference): ResolvedTheme {
  if (preference === 'system') return getSystemTheme();
  return preference;
}

export function readStoredPreference(): ThemePreference {
  if (typeof window === 'undefined') return 'system';
  try {
    const raw = window.localStorage.getItem(THEME_STORAGE_KEY);
    return isThemePreference(raw) ? raw : 'system';
  } catch {
    return 'system';
  }
}

export function writeStoredPreference(preference: ThemePreference): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, preference);
  } catch {
    /* storage may be unavailable (private mode, quota); ignore. */
  }
}

export function applyResolvedTheme(theme: ResolvedTheme): void {
  if (typeof document === 'undefined') return;
  document.documentElement.dataset.theme = theme;
}

export function applyPreference(preference: ThemePreference): ResolvedTheme {
  const resolved = resolveTheme(preference);
  applyResolvedTheme(resolved);
  return resolved;
}

/**
 * Inline script string injected into <head> before hydration.
 * Reads stored preference (or system query) and sets `data-theme` on <html>
 * synchronously, eliminating FOUC.
 */
export const THEME_INIT_SCRIPT = `(() => {
  try {
    var key = ${JSON.stringify(THEME_STORAGE_KEY)};
    var stored = null;
    try { stored = window.localStorage.getItem(key); } catch (_) {}
    var pref = (stored === 'light' || stored === 'dark' || stored === 'system') ? stored : 'system';
    var resolved = pref;
    if (pref === 'system') {
      var mq = window.matchMedia('(prefers-color-scheme: dark)');
      resolved = mq && mq.matches ? 'dark' : 'light';
    }
    document.documentElement.dataset.theme = resolved;
  } catch (_) {
    document.documentElement.dataset.theme = 'light';
  }
})();`;
