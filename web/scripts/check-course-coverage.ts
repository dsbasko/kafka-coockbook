#!/usr/bin/env tsx
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  buildCoverageReport,
  formatCoverageReport,
  isCoverageFailing,
} from '../lib/coverage';
import { loadCourse } from '../lib/course-loader';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const LECTURES_DIR = path.join(REPO_ROOT, 'lectures');
const COURSE_YAML = path.join(REPO_ROOT, 'course.yaml');

function main(): void {
  const course = loadCourse('ru', {
    filePath: COURSE_YAML,
    lecturesRoot: LECTURES_DIR,
  });
  const report = buildCoverageReport(course, LECTURES_DIR);
  const formatted = formatCoverageReport(report);

  for (const line of formatted.ok) console.log(line);
  for (const line of formatted.translationGaps) console.log(line);
  for (const line of formatted.mismatches) console.error(line);
  console.error(`\n${formatted.summary}`);

  process.exit(isCoverageFailing(report) ? 1 : 0);
}

main();
