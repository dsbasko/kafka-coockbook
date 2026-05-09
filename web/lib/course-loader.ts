import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { parseCourse, type Course } from './course';

// Try a few known-good locations so the loader works whether Node was
// launched from web/ (typical: pnpm scripts, vitest, next build) or from
// the repo root (e.g. `node web/dist/...`). First hit wins.
function defaultCoursePath(): string {
  const candidates = [
    path.resolve(process.cwd(), '..', 'course.yaml'), // CWD=web/
    path.resolve(process.cwd(), 'course.yaml'), // CWD=repo root
  ];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  return candidates[0];
}

export function loadCourse(filePath: string = defaultCoursePath()): Course {
  const raw = readFileSync(filePath, 'utf8');
  return parseCourse(raw, filePath);
}
