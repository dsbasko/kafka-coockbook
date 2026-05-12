import { promises as fs } from 'node:fs';
import path from 'node:path';
import { DEFAULT_LANG, type Lang } from './lang';

export interface LessonContent {
  markdown: string;
  /**
   * The language actually loaded. May differ from the requested lang when the
   * requested translation is missing and the loader falls back (currently to
   * Russian) — callers use this to decide whether to render a fallback banner.
   */
  lang: Lang;
  /**
   * True when the requested language was not available and the loader fell
   * back to a different one. Always false when `lang === requested`.
   */
  fallbackUsed: boolean;
}

const DEFAULT_LECTURES_ROOT = path.resolve(process.cwd(), '..', 'lectures');

export interface GetLessonContentOptions {
  lecturesRoot?: string;
}

export async function getLessonContent(
  moduleId: string,
  slug: string,
  lang: Lang = DEFAULT_LANG,
  options: GetLessonContentOptions = {},
): Promise<LessonContent> {
  const root = options.lecturesRoot ?? DEFAULT_LECTURES_ROOT;
  const primaryPath = getLessonReadmePath(moduleId, slug, lang, options);

  try {
    const markdown = await fs.readFile(primaryPath, 'utf8');
    return { markdown, lang, fallbackUsed: false };
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== 'ENOENT') throw err;
  }

  if (lang !== 'ru') {
    const fallbackPath = getLessonReadmePath(moduleId, slug, 'ru', { lecturesRoot: root });
    try {
      const markdown = await fs.readFile(fallbackPath, 'utf8');
      return { markdown, lang: 'ru', fallbackUsed: true };
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== 'ENOENT') throw err;
    }
  }

  throw new Error(
    `lesson README not found: ${moduleId}/${slug} (expected at ${primaryPath})`,
  );
}

export function getLessonReadmePath(
  moduleId: string,
  slug: string,
  lang: Lang = DEFAULT_LANG,
  options: GetLessonContentOptions = {},
): string {
  const root = options.lecturesRoot ?? DEFAULT_LECTURES_ROOT;
  return path.join(root, moduleId, slug, 'i18n', lang, 'README.md');
}
