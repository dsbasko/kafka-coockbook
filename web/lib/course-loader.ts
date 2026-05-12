import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { parseCourse, type Course } from './course';
import { type Lang } from './lang';

// Try a few known-good locations so the loader works whether Node was
// launched from web/ (typical: pnpm scripts, vitest, next build) or from
// the repo root (e.g. `node web/dist/...`). Probe `./course.yaml` before
// `../course.yaml` so a stray manifest in the parent of the repo never wins
// over the one inside the repo when run from the repo root.
function defaultCoursePath(): string {
  const candidates = [
    path.resolve(process.cwd(), 'course.yaml'), // CWD=repo root
    path.resolve(process.cwd(), '..', 'course.yaml'), // CWD=web/
  ];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  return candidates[0];
}

function defaultLecturesRoot(): string {
  const candidates = [
    path.resolve(process.cwd(), 'lectures'),
    path.resolve(process.cwd(), '..', 'lectures'),
  ];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  return candidates[0];
}

export interface LoadCourseOptions {
  filePath?: string;
  lecturesRoot?: string;
}

export function loadCourse(lang: Lang, options: LoadCourseOptions = {}): Course {
  const filePath = options.filePath ?? defaultCoursePath();
  const lecturesRoot = options.lecturesRoot ?? defaultLecturesRoot();
  const raw = readFileSync(filePath, 'utf8');
  const course = parseCourse(raw, lang, filePath);

  for (const mod of course.modules) {
    for (const lesson of mod.lessons) {
      const translationPath = path.join(
        lecturesRoot,
        mod.id,
        lesson.slug,
        'i18n',
        lang,
        'README.md',
      );
      lesson.hasTranslation = existsSync(translationPath);
    }
  }

  return course;
}
