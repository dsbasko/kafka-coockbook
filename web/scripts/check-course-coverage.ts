#!/usr/bin/env tsx
import { existsSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { type Course } from '../lib/course';
import { loadCourse } from '../lib/course-loader';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const LECTURES_DIR = path.join(REPO_ROOT, 'lectures');
const COURSE_YAML = path.join(REPO_ROOT, 'course.yaml');

interface Row {
  status: 'OK' | 'MISSING_IN_YAML' | 'MISSING_IN_FS';
  moduleId: string;
  slug: string;
}

function discoverFromFs(): Set<string> {
  const out = new Set<string>();
  for (const moduleEntry of readdirSync(LECTURES_DIR)) {
    const moduleDir = path.join(LECTURES_DIR, moduleEntry);
    if (!statSync(moduleDir).isDirectory()) continue;
    if (!/^\d{2}-/.test(moduleEntry)) continue;

    for (const lessonEntry of readdirSync(moduleDir)) {
      const lessonDir = path.join(moduleDir, lessonEntry);
      if (!statSync(lessonDir).isDirectory()) continue;
      const readmePath = path.join(lessonDir, 'README.md');
      if (!existsSync(readmePath)) continue;
      out.add(`${moduleEntry}/${lessonEntry}`);
    }
  }
  return out;
}

function discoverFromCourse(course: Course): Set<string> {
  const out = new Set<string>();
  for (const mod of course.modules) {
    for (const lesson of mod.lessons) {
      out.add(`${mod.id}/${lesson.slug}`);
    }
  }
  return out;
}

function main(): void {
  const course = loadCourse('ru', { filePath: COURSE_YAML });
  const fsSet = discoverFromFs();
  const yamlSet = discoverFromCourse(course);

  const allKeys = new Set<string>([...fsSet, ...yamlSet]);
  const rows: Row[] = [];

  for (const key of [...allKeys].sort()) {
    const [moduleId, slug] = key.split('/', 2);
    const inFs = fsSet.has(key);
    const inYaml = yamlSet.has(key);
    if (inFs && inYaml) {
      rows.push({ status: 'OK', moduleId, slug });
    } else if (inFs && !inYaml) {
      rows.push({ status: 'MISSING_IN_YAML', moduleId, slug });
    } else {
      rows.push({ status: 'MISSING_IN_FS', moduleId, slug });
    }
  }

  let mismatchCount = 0;
  for (const row of rows) {
    const line = `${row.status} ${row.moduleId}/${row.slug}`;
    if (row.status === 'OK') {
      console.log(line);
    } else {
      console.error(line);
      mismatchCount += 1;
    }
  }

  console.error(
    `\nTotal: ${rows.length}, OK: ${rows.length - mismatchCount}, mismatches: ${mismatchCount}`,
  );
  process.exit(mismatchCount === 0 ? 0 : 1);
}

main();
