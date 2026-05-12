export type Lang = 'ru' | 'en';

export const LANGS: readonly Lang[] = ['ru', 'en'] as const;
export const DEFAULT_LANG: Lang = 'en';
export const LANG_STORAGE_KEY = 'kafka-cookbook-lang';

export const LANG_LABELS: Record<Lang, string> = {
  ru: 'Русский',
  en: 'English',
};

export function isLang(value: unknown): value is Lang {
  return value === 'ru' || value === 'en';
}

export function getBrowserLang(): Lang {
  if (typeof navigator === 'undefined') return DEFAULT_LANG;
  const raw = navigator.language || (navigator.languages && navigator.languages[0]) || '';
  return raw.toLowerCase().startsWith('ru') ? 'ru' : 'en';
}

export function readStoredLang(): Lang | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.localStorage.getItem(LANG_STORAGE_KEY);
    return isLang(raw) ? raw : null;
  } catch {
    return null;
  }
}

export function writeStoredLang(lang: Lang): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch {
    /* storage may be unavailable (private mode, quota); ignore. */
  }
}

/**
 * Strips a leading `/ru/` or `/en/` segment from a pathname. Used by URL
 * parsers (LessonAwareLink, GateProvider, gate-scripts) so the remaining
 * segments are `[moduleId, slug]` regardless of the active language.
 *
 * Examples:
 *   /ru/foo/bar  -> { lang: 'ru', rest: '/foo/bar' }
 *   /en/         -> { lang: 'en', rest: '/' }
 *   /ru          -> { lang: 'ru', rest: '/' }
 *   /foo/bar     -> { lang: null, rest: '/foo/bar' }
 *   /enfoo       -> { lang: null, rest: '/enfoo' }
 *   ''           -> { lang: null, rest: '/' }
 */
export function stripLangFromPath(pathname: string): { lang: Lang | null; rest: string } {
  if (!pathname) return { lang: null, rest: '/' };
  const match = pathname.match(/^\/(ru|en)(\/.*)?$/);
  if (!match) return { lang: null, rest: pathname };
  const lang = match[1] as Lang;
  const rest = match[2] && match[2].length > 0 ? match[2] : '/';
  return { lang, rest };
}

/**
 * Inline script for the root `/page.tsx`. Decides whether to redirect the
 * visitor to `/{lang}/` based on (1) stored preference, (2) navigator.language.
 * If the resolved language equals DEFAULT_LANG we skip the redirect to avoid
 * a flash — the static `/index.html` already serves DEFAULT_LANG content.
 *
 * Side effect: when the language is auto-detected (no stored value yet) we
 * persist the choice so subsequent visits are stable.
 *
 * basePath is derived from `window.location.pathname`: the root index.html
 * sits at `/{basePath}/`, so appending `{lang}/` to that path yields the
 * correct redirect target without needing a build-time substitution.
 */
export const LANG_INIT_SCRIPT = `(function(){try{var key=${JSON.stringify(
  LANG_STORAGE_KEY,
)};var def=${JSON.stringify(
  DEFAULT_LANG,
)};var stored=null;try{stored=window.localStorage.getItem(key);}catch(e){}var lang;if(stored==='ru'||stored==='en'){lang=stored;}else{var nav=(navigator&&(navigator.language||(navigator.languages&&navigator.languages[0])))||'';lang=String(nav).toLowerCase().indexOf('ru')===0?'ru':'en';try{window.localStorage.setItem(key,lang);}catch(e){}}if(lang===def)return;var p=window.location.pathname;if(p.charAt(p.length-1)!=='/')p+='/';window.location.replace(p+lang+'/');}catch(e){}})();`;

/**
 * Inline script for `[lang]/layout.tsx`. Syncs `document.documentElement.lang`
 * to the active language so in-browser APIs (Intl, screen readers) see the
 * correct value — the static HTML always carries `<html lang={DEFAULT_LANG}>`
 * because Next.js renders <html> once at the root layout.
 */
export function LANG_SYNC_SCRIPT(lang: Lang): string {
  return `(function(){try{document.documentElement.lang=${JSON.stringify(lang)};}catch(e){}})();`;
}
