import yaml from 'js-yaml';
import { type Lang } from './lang';
import { isValidSlug } from './slug';

export interface Lesson {
  slug: string;
  title: string;
  duration: string;
  tags: string[];
  hasTranslation: boolean;
}

export interface Module {
  id: string;
  title: string;
  description: string;
  lessons: Lesson[];
}

export interface Course {
  title: string;
  description: string;
  basePath: string;
  repoUrl: string;
  modules: Module[];
}

export interface FlatLessonEntry {
  moduleId: string;
  lesson: Lesson;
  index: number;
}

export function parseCourse(
  source: string,
  lang: Lang = 'ru',
  sourcePath = '<inline>',
): Course {
  let parsed: unknown;
  try {
    parsed = yaml.load(source);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`course.yaml: invalid YAML in ${sourcePath}: ${message}`);
  }

  if (!isPlainObject(parsed)) {
    throw new Error(`course.yaml: expected top-level mapping in ${sourcePath}`);
  }

  const title = requireLocalized(parsed, 'title', lang);
  const description = requireLocalized(parsed, 'description', lang);
  const basePath = requireString(parsed, 'basePath');
  const repoUrl = requireString(parsed, 'repoUrl');

  const rawModules = parsed.modules;
  if (!Array.isArray(rawModules) || rawModules.length === 0) {
    throw new Error(`course.yaml: "modules" must be a non-empty array`);
  }

  const seenModuleIds = new Set<string>();
  const modules: Module[] = rawModules.map((value, index) => {
    const where = `modules[${index}]`;
    if (!isPlainObject(value)) {
      throw new Error(`course.yaml: ${where} must be a mapping`);
    }

    const id = requireString(value, 'id', where);
    if (!isValidSlug(id)) {
      throw new Error(`course.yaml: ${where}.id "${id}" is not a valid slug`);
    }
    if (seenModuleIds.has(id)) {
      throw new Error(`course.yaml: duplicate module id "${id}"`);
    }
    seenModuleIds.add(id);

    const moduleTitle = requireLocalized(value, 'title', lang, where);
    const moduleDescription = requireLocalized(value, 'description', lang, where);

    const rawLessons = value.lessons;
    if (!Array.isArray(rawLessons) || rawLessons.length === 0) {
      throw new Error(`course.yaml: ${where}.lessons must be a non-empty array`);
    }

    const seenSlugs = new Set<string>();
    const lessons: Lesson[] = rawLessons.map((lessonValue, lessonIndex) => {
      const lessonWhere = `${where}.lessons[${lessonIndex}]`;
      if (!isPlainObject(lessonValue)) {
        throw new Error(`course.yaml: ${lessonWhere} must be a mapping`);
      }
      const slug = requireString(lessonValue, 'slug', lessonWhere);
      if (!isValidSlug(slug)) {
        throw new Error(`course.yaml: ${lessonWhere}.slug "${slug}" is not a valid slug`);
      }
      if (seenSlugs.has(slug)) {
        throw new Error(`course.yaml: duplicate lesson slug "${slug}" in module "${id}"`);
      }
      seenSlugs.add(slug);

      const lessonTitle = requireLocalized(lessonValue, 'title', lang, lessonWhere);
      const duration = requireString(lessonValue, 'duration', lessonWhere);
      const tags = parseTags(lessonValue.tags, lessonWhere);

      return { slug, title: lessonTitle, duration, tags, hasTranslation: false };
    });

    return {
      id,
      title: moduleTitle,
      description: moduleDescription,
      lessons,
    };
  });

  return { title, description, basePath, repoUrl, modules };
}

export function findLesson(
  course: Course,
  moduleId: string,
  lessonSlug: string,
): Lesson | null {
  const mod = course.modules.find((m) => m.id === moduleId);
  if (!mod) return null;
  return mod.lessons.find((l) => l.slug === lessonSlug) ?? null;
}

export function getLessonIndex(
  course: Course,
  moduleId: string,
  slug: string,
): number {
  const flat = flattenLessons(course);
  return flat.findIndex((e) => e.moduleId === moduleId && e.lesson.slug === slug);
}

export function flattenLessons(course: Course): FlatLessonEntry[] {
  const result: FlatLessonEntry[] = [];
  let index = 0;
  for (const mod of course.modules) {
    for (const lesson of mod.lessons) {
      result.push({ moduleId: mod.id, lesson, index });
      index += 1;
    }
  }
  return result;
}

export function getNextLesson(
  course: Course,
  moduleId: string,
  slug: string,
): FlatLessonEntry | null {
  const flat = flattenLessons(course);
  const idx = flat.findIndex((e) => e.moduleId === moduleId && e.lesson.slug === slug);
  if (idx === -1 || idx === flat.length - 1) return null;
  return flat[idx + 1];
}

export function getPrevLesson(
  course: Course,
  moduleId: string,
  slug: string,
): FlatLessonEntry | null {
  const flat = flattenLessons(course);
  const idx = flat.findIndex((e) => e.moduleId === moduleId && e.lesson.slug === slug);
  if (idx <= 0) return null;
  return flat[idx - 1];
}

export function getTotalLessons(course: Course): number {
  let total = 0;
  for (const mod of course.modules) total += mod.lessons.length;
  return total;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requireString(
  obj: Record<string, unknown>,
  key: string,
  where = '<root>',
): string {
  const value = obj[key];
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`course.yaml: ${where}.${key} is required and must be a non-empty string`);
  }
  return value.trim();
}

function requireLocalized(
  obj: Record<string, unknown>,
  key: string,
  lang: Lang,
  where = '<root>',
): string {
  const value = obj[key];
  if (typeof value === 'string') {
    if (value.trim().length === 0) {
      throw new Error(
        `course.yaml: ${where}.${key} is required and must be a non-empty string`,
      );
    }
    return value.trim();
  }
  if (isPlainObject(value)) {
    const ruRaw = value.ru;
    if (typeof ruRaw !== 'string' || ruRaw.trim().length === 0) {
      throw new Error(
        `course.yaml: ${where}.${key}.ru is required when ${key} is a locale map`,
      );
    }
    const target = value[lang];
    if (typeof target === 'string' && target.trim().length > 0) {
      return target.trim();
    }
    return ruRaw.trim();
  }
  throw new Error(
    `course.yaml: ${where}.${key} must be a non-empty string or a { ru, en } locale map`,
  );
}

function parseTags(value: unknown, where: string): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw new Error(`course.yaml: ${where}.tags must be an array of strings`);
  }
  return value.map((item, i) => {
    if (typeof item !== 'string' || item.length === 0) {
      throw new Error(`course.yaml: ${where}.tags[${i}] must be a non-empty string`);
    }
    return item;
  });
}
