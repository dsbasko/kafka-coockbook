import { existsSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import type { Course } from './course';
import { LANGS, type Lang } from './lang';

export type CoverageStatus = 'OK' | 'MISSING_IN_YAML' | 'MISSING_IN_FS';

export interface CoverageRow {
  status: CoverageStatus;
  moduleId: string;
  slug: string;
  translations: Record<Lang, boolean>;
}

export interface CoverageTotals {
  lessons: number;
  mismatches: number;
  translations: Record<Lang, number>;
}

export interface CoverageReport {
  rows: CoverageRow[];
  totals: CoverageTotals;
}

export interface FormattedCoverage {
  ok: string[];
  translationGaps: string[];
  mismatches: string[];
  summary: string;
}

export function discoverFromFs(lecturesRoot: string): Set<string> {
  const out = new Set<string>();
  if (!existsSync(lecturesRoot)) return out;
  for (const moduleEntry of readdirSync(lecturesRoot)) {
    const moduleDir = path.join(lecturesRoot, moduleEntry);
    if (!statSync(moduleDir).isDirectory()) continue;
    if (!/^\d{2}-/.test(moduleEntry)) continue;

    for (const lessonEntry of readdirSync(moduleDir)) {
      const lessonDir = path.join(moduleDir, lessonEntry);
      if (!statSync(lessonDir).isDirectory()) continue;
      const stub = path.join(lessonDir, 'README.md');
      const ruIndex = path.join(lessonDir, 'i18n', 'ru', 'README.md');
      if (!existsSync(stub) && !existsSync(ruIndex)) continue;
      out.add(`${moduleEntry}/${lessonEntry}`);
    }
  }
  return out;
}

export function discoverFromCourse(course: Course): Set<string> {
  const out = new Set<string>();
  for (const mod of course.modules) {
    for (const lesson of mod.lessons) {
      out.add(`${mod.id}/${lesson.slug}`);
    }
  }
  return out;
}

export function buildCoverageReport(
  course: Course,
  lecturesRoot: string,
): CoverageReport {
  const fsSet = discoverFromFs(lecturesRoot);
  const yamlSet = discoverFromCourse(course);
  const allKeys = new Set<string>([...fsSet, ...yamlSet]);
  const rows: CoverageRow[] = [];

  for (const key of [...allKeys].sort()) {
    const [moduleId, slug] = key.split('/', 2);
    const inFs = fsSet.has(key);
    const inYaml = yamlSet.has(key);
    let status: CoverageStatus;
    if (inFs && inYaml) status = 'OK';
    else if (inFs) status = 'MISSING_IN_YAML';
    else status = 'MISSING_IN_FS';

    const translations: Record<Lang, boolean> = { ru: false, en: false };
    for (const lang of LANGS) {
      const file = path.join(
        lecturesRoot,
        moduleId,
        slug,
        'i18n',
        lang,
        'README.md',
      );
      translations[lang] = existsSync(file);
    }
    rows.push({ status, moduleId, slug, translations });
  }

  const okRows = rows.filter((r) => r.status === 'OK');
  const translationsTotals: Record<Lang, number> = { ru: 0, en: 0 };
  for (const lang of LANGS) {
    translationsTotals[lang] = okRows.filter((r) => r.translations[lang]).length;
  }

  return {
    rows,
    totals: {
      lessons: okRows.length,
      mismatches: rows.length - okRows.length,
      translations: translationsTotals,
    },
  };
}

export function formatCoverageReport(report: CoverageReport): FormattedCoverage {
  const ok: string[] = [];
  const translationGaps: string[] = [];
  const mismatches: string[] = [];

  for (const row of report.rows) {
    const key = `${row.moduleId}/${row.slug}`;
    if (row.status !== 'OK') {
      mismatches.push(`${row.status} ${key}`);
      continue;
    }
    const tags: string[] = [];
    if (!row.translations.ru) tags.push('NO_RU');
    if (!row.translations.en) tags.push('NO_EN');
    if (tags.length > 0) {
      translationGaps.push(`OK ${key} [${tags.join(', ')}]`);
    } else {
      ok.push(`OK ${key}`);
    }
  }

  const { lessons, mismatches: mismatchCount, translations } = report.totals;
  const summary = `Lessons: ${lessons} | RU: ${translations.ru}/${lessons} | EN: ${translations.en}/${lessons} | mismatches: ${mismatchCount}`;
  return { ok, translationGaps, mismatches, summary };
}

export function isCoverageFailing(report: CoverageReport): boolean {
  if (report.totals.mismatches > 0) return true;
  return report.totals.translations.ru < report.totals.lessons;
}
