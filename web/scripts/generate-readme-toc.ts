#!/usr/bin/env tsx
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadCourse } from '../lib/course-loader';
import { isLang, type Lang } from '../lib/lang';
import { generateReadmeToc } from '../lib/readme-toc';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const LECTURES_DIR = path.join(REPO_ROOT, 'lectures');
const COURSE_YAML = path.join(REPO_ROOT, 'course.yaml');

function parseLang(): Lang {
  const arg = process.argv.slice(2).find((a) => a.startsWith('--lang='));
  if (!arg) return 'en';
  const value = arg.slice('--lang='.length);
  if (!isLang(value)) {
    console.error(`Invalid --lang value: ${value}. Expected 'ru' or 'en'.`);
    process.exit(2);
  }
  return value;
}

function main(): void {
  const lang = parseLang();
  const course = loadCourse(lang, {
    filePath: COURSE_YAML,
    lecturesRoot: LECTURES_DIR,
  });
  process.stdout.write(generateReadmeToc(course, { lang, lecturesRoot: LECTURES_DIR }));
}

main();
