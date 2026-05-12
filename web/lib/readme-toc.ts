import { existsSync } from 'node:fs';
import path from 'node:path';
import type { Course, Module, Lesson } from './course';
import { type Lang } from './lang';

export interface GenerateTocOptions {
  /** Active language. EN tries to link to `i18n/en/README.md` and falls back to RU with a marker. */
  lang: Lang;
  /** Path prefix used in markdown link targets (relative to the README that will embed the TOC). Default: 'lectures'. */
  linkPrefix?: string;
  /** Filesystem root used to probe which translations exist. Default: same as linkPrefix. */
  lecturesRoot?: string;
}

const RU_ONLY_MARKER = ' *(RU only)*';

export function generateReadmeToc(course: Course, options: GenerateTocOptions): string {
  const linkPrefix = options.linkPrefix ?? 'lectures';
  const fsRoot = options.lecturesRoot ?? linkPrefix;
  const sections = course.modules.map((mod) =>
    renderModule(mod, options.lang, linkPrefix, fsRoot),
  );
  return sections.join('\n\n').replace(/\s+$/, '') + '\n';
}

function renderModule(mod: Module, lang: Lang, linkPrefix: string, fsRoot: string): string {
  const moduleNumber = leadingNumber(mod.id);
  const heading = `### ${moduleNumber} — ${mod.title}`;
  const description = mod.description.trim();
  const bullets = mod.lessons
    .map((lesson) => renderLesson(mod.id, moduleNumber, lesson, lang, linkPrefix, fsRoot))
    .join('\n');
  return [heading, '', description, '', bullets].join('\n');
}

function renderLesson(
  moduleId: string,
  moduleNumber: string,
  lesson: Lesson,
  lang: Lang,
  linkPrefix: string,
  fsRoot: string,
): string {
  const enFile = path.join(fsRoot, moduleId, lesson.slug, 'i18n', 'en', 'README.md');
  const hasEn = existsSync(enFile);
  const targetLang: Lang = lang === 'en' && !hasEn ? 'ru' : lang;
  const linkTarget = `${linkPrefix}/${moduleId}/${lesson.slug}/i18n/${targetLang}/README.md`;
  const label = buildLessonLabel(moduleNumber, lesson);
  const marker = lang === 'en' && !hasEn ? RU_ONLY_MARKER : '';
  return `- [${label}](${linkTarget})${marker}`;
}

function buildLessonLabel(moduleNumber: string, lesson: Lesson): string {
  const lessonNum = leadingLessonNumber(lesson.slug);
  if (!lessonNum) return lesson.title;
  return `${moduleNumber}-${lessonNum} — ${lesson.title}`;
}

function leadingNumber(id: string): string {
  const m = id.match(/^(\d{2})/);
  return m ? m[1] : id;
}

function leadingLessonNumber(slug: string): string | null {
  const nested = slug.match(/^\d{2}-(\d{2})-/);
  if (nested) return nested[1];
  const flat = slug.match(/^(\d{2})-/);
  return flat ? flat[1] : null;
}
