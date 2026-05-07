import { readFileSync } from 'node:fs';
import path from 'node:path';
import { parseCourse, type Course } from './course';

const DEFAULT_COURSE_PATH = path.resolve(process.cwd(), '..', 'course.yaml');

export function loadCourse(filePath: string = DEFAULT_COURSE_PATH): Course {
  const raw = readFileSync(filePath, 'utf8');
  return parseCourse(raw, filePath);
}
