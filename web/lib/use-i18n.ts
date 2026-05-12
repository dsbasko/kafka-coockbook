'use client';

import { useParams } from 'next/navigation';
import { DEFAULT_LANG, isLang, type Lang } from './lang';
import { getDict, type UIDict } from './i18n';

/**
 * Reads the active language from the `[lang]` segment of the URL. Falls
 * back to DEFAULT_LANG if the param is missing or invalid — happens on
 * the root layout before the language-prefixed route mounts.
 */
export function useLang(): Lang {
  const params = useParams();
  const raw = params && (params as Record<string, string | string[] | undefined>).lang;
  const value = Array.isArray(raw) ? raw[0] : raw;
  return isLang(value) ? value : DEFAULT_LANG;
}

/**
 * Client-side translation accessor. Returns the full dictionary for the
 * active language so component code reads `t.something` directly.
 */
export function useT(): UIDict {
  return getDict(useLang());
}
