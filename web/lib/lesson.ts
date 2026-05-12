import { promises as fs } from 'node:fs';
import path from 'node:path';

export interface LessonContent {
  markdown: string;
}

const DEFAULT_LECTURES_ROOT = path.resolve(process.cwd(), '..', 'lectures');

export interface GetLessonContentOptions {
  lecturesRoot?: string;
}

export async function getLessonContent(
  moduleId: string,
  slug: string,
  options: GetLessonContentOptions = {},
): Promise<LessonContent> {
  const readmePath = getLessonReadmePath(moduleId, slug, options);
  try {
    const markdown = await fs.readFile(readmePath, 'utf8');
    return { markdown };
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ENOENT') {
      throw new Error(
        `lesson README not found: ${moduleId}/${slug} (expected at ${readmePath})`,
      );
    }
    throw err;
  }
}

export function getLessonReadmePath(
  moduleId: string,
  slug: string,
  options: GetLessonContentOptions = {},
): string {
  const root = options.lecturesRoot ?? DEFAULT_LECTURES_ROOT;
  return path.join(root, moduleId, slug, 'i18n', 'ru', 'README.md');
}
