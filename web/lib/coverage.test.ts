import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { Course, Lesson, Module } from './course';
import {
  buildCoverageReport,
  discoverFromCourse,
  discoverFromFs,
  formatCoverageReport,
  isCoverageFailing,
} from './coverage';

let workDir: string;

function makeLesson(slug: string): Lesson {
  return { slug, title: slug, duration: '30m', tags: [], hasTranslation: false };
}

function makeModule(id: string, lessons: Lesson[]): Module {
  return { id, title: id, description: `${id} desc`, lessons };
}

function makeCourse(modules: Module[]): Course {
  return {
    title: 'Test',
    description: 'Test desc',
    basePath: '/test',
    repoUrl: 'https://example.com/test',
    modules,
  };
}

function writeFsLesson(
  moduleId: string,
  slug: string,
  options: { ru?: boolean; en?: boolean; stub?: boolean } = {},
): void {
  const lessonDir = path.join(workDir, moduleId, slug);
  mkdirSync(lessonDir, { recursive: true });
  if (options.stub ?? true) {
    writeFileSync(path.join(lessonDir, 'README.md'), '# stub\n');
  }
  if (options.ru) {
    const ruDir = path.join(lessonDir, 'i18n', 'ru');
    mkdirSync(ruDir, { recursive: true });
    writeFileSync(path.join(ruDir, 'README.md'), '# ru\n');
  }
  if (options.en) {
    const enDir = path.join(lessonDir, 'i18n', 'en');
    mkdirSync(enDir, { recursive: true });
    writeFileSync(path.join(enDir, 'README.md'), '# en\n');
  }
}

beforeEach(() => {
  workDir = mkdtempSync(path.join(tmpdir(), 'coverage-'));
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

describe('discoverFromFs', () => {
  it('returns an empty set when the lectures root does not exist', () => {
    const set = discoverFromFs(path.join(workDir, 'missing'));
    expect(set.size).toBe(0);
  });

  it('discovers lessons through stub README.md', () => {
    writeFsLesson('01-foo', '01-01-intro', { stub: true });
    const set = discoverFromFs(workDir);
    expect([...set]).toEqual(['01-foo/01-01-intro']);
  });

  it('discovers lessons through i18n/ru/README.md when stub is missing', () => {
    writeFsLesson('01-foo', '01-02-deep', { stub: false, ru: true });
    const set = discoverFromFs(workDir);
    expect([...set]).toEqual(['01-foo/01-02-deep']);
  });

  it('skips directories that do not start with two digits', () => {
    writeFsLesson('docs', '01-01-intro', { stub: true });
    writeFsLesson('01-foo', '01-01-intro', { stub: true });
    const set = discoverFromFs(workDir);
    expect([...set]).toEqual(['01-foo/01-01-intro']);
  });

  it('skips lesson directories without any README marker', () => {
    const lessonDir = path.join(workDir, '01-foo', '01-01-empty');
    mkdirSync(lessonDir, { recursive: true });
    const set = discoverFromFs(workDir);
    expect(set.size).toBe(0);
  });
});

describe('discoverFromCourse', () => {
  it('returns module/slug pairs from the course manifest', () => {
    const course = makeCourse([
      makeModule('01-foo', [makeLesson('01-01-intro'), makeLesson('01-02-deep')]),
      makeModule('02-bar', [makeLesson('02-01-start')]),
    ]);
    const set = discoverFromCourse(course);
    expect([...set].sort()).toEqual([
      '01-foo/01-01-intro',
      '01-foo/01-02-deep',
      '02-bar/02-01-start',
    ]);
  });
});

describe('buildCoverageReport', () => {
  it('marks rows OK when both manifest and FS agree, with translation flags', () => {
    const course = makeCourse([
      makeModule('01-foo', [makeLesson('01-01-intro'), makeLesson('01-02-deep')]),
    ]);
    writeFsLesson('01-foo', '01-01-intro', { ru: true, en: true });
    writeFsLesson('01-foo', '01-02-deep', { ru: true });

    const report = buildCoverageReport(course, workDir);
    expect(report.rows).toHaveLength(2);
    expect(report.rows[0]).toMatchObject({
      status: 'OK',
      moduleId: '01-foo',
      slug: '01-01-intro',
      translations: { ru: true, en: true },
    });
    expect(report.rows[1]).toMatchObject({
      status: 'OK',
      slug: '01-02-deep',
      translations: { ru: true, en: false },
    });
    expect(report.totals).toEqual({
      lessons: 2,
      mismatches: 0,
      translations: { ru: 2, en: 1 },
    });
  });

  it('reports MISSING_IN_FS when manifest lists a lesson that has no directory', () => {
    const course = makeCourse([
      makeModule('01-foo', [makeLesson('01-01-intro'), makeLesson('01-02-ghost')]),
    ]);
    writeFsLesson('01-foo', '01-01-intro', { ru: true });

    const report = buildCoverageReport(course, workDir);
    const ghost = report.rows.find((r) => r.slug === '01-02-ghost');
    expect(ghost?.status).toBe('MISSING_IN_FS');
    expect(report.totals.mismatches).toBe(1);
    expect(report.totals.lessons).toBe(1);
  });

  it('reports MISSING_IN_YAML when FS has a lesson absent from the manifest', () => {
    const course = makeCourse([makeModule('01-foo', [makeLesson('01-01-intro')])]);
    writeFsLesson('01-foo', '01-01-intro', { ru: true });
    writeFsLesson('01-foo', '01-99-extra', { ru: true });

    const report = buildCoverageReport(course, workDir);
    const extra = report.rows.find((r) => r.slug === '01-99-extra');
    expect(extra?.status).toBe('MISSING_IN_YAML');
    expect(report.totals.mismatches).toBe(1);
  });

  it('counts only OK rows in translation totals', () => {
    const course = makeCourse([
      makeModule('01-foo', [makeLesson('01-01-intro'), makeLesson('01-02-ghost')]),
    ]);
    writeFsLesson('01-foo', '01-01-intro', { ru: true, en: true });
    // 01-02-ghost has no FS entry → MISSING_IN_FS, must not count toward translations.

    const report = buildCoverageReport(course, workDir);
    expect(report.totals).toEqual({
      lessons: 1,
      mismatches: 1,
      translations: { ru: 1, en: 1 },
    });
  });

  it('keeps rows sorted by module/slug for stable output', () => {
    const course = makeCourse([
      makeModule('02-bar', [makeLesson('02-01-start')]),
      makeModule('01-foo', [makeLesson('01-02-deep'), makeLesson('01-01-intro')]),
    ]);
    writeFsLesson('02-bar', '02-01-start', { ru: true });
    writeFsLesson('01-foo', '01-01-intro', { ru: true });
    writeFsLesson('01-foo', '01-02-deep', { ru: true });

    const report = buildCoverageReport(course, workDir);
    expect(report.rows.map((r) => `${r.moduleId}/${r.slug}`)).toEqual([
      '01-foo/01-01-intro',
      '01-foo/01-02-deep',
      '02-bar/02-01-start',
    ]);
  });
});

describe('formatCoverageReport', () => {
  it('separates OK, translation gaps, and mismatches into distinct buckets', () => {
    const course = makeCourse([
      makeModule('01-foo', [makeLesson('01-01-intro'), makeLesson('01-02-deep')]),
      makeModule('02-bar', [makeLesson('02-01-ghost')]),
    ]);
    writeFsLesson('01-foo', '01-01-intro', { ru: true, en: true });
    writeFsLesson('01-foo', '01-02-deep', { ru: true });

    const formatted = formatCoverageReport(buildCoverageReport(course, workDir));
    expect(formatted.ok).toEqual(['OK 01-foo/01-01-intro']);
    expect(formatted.translationGaps).toEqual(['OK 01-foo/01-02-deep [NO_EN]']);
    expect(formatted.mismatches).toEqual(['MISSING_IN_FS 02-bar/02-01-ghost']);
    expect(formatted.summary).toBe(
      'Lessons: 2 | RU: 2/2 | EN: 1/2 | mismatches: 1',
    );
  });

  it('flags lessons missing both translations with NO_RU and NO_EN', () => {
    const course = makeCourse([makeModule('01-foo', [makeLesson('01-01-intro')])]);
    writeFsLesson('01-foo', '01-01-intro', { stub: true });

    const formatted = formatCoverageReport(buildCoverageReport(course, workDir));
    expect(formatted.translationGaps).toEqual([
      'OK 01-foo/01-01-intro [NO_RU, NO_EN]',
    ]);
  });
});

describe('isCoverageFailing', () => {
  it('returns true when there is any structural mismatch', () => {
    const course = makeCourse([makeModule('01-foo', [makeLesson('01-01-ghost')])]);
    expect(isCoverageFailing(buildCoverageReport(course, workDir))).toBe(true);
  });

  it('returns true when an OK lesson is missing the RU translation', () => {
    const course = makeCourse([makeModule('01-foo', [makeLesson('01-01-intro')])]);
    writeFsLesson('01-foo', '01-01-intro', { stub: true, en: true });
    expect(isCoverageFailing(buildCoverageReport(course, workDir))).toBe(true);
  });

  it('returns false when all lessons have RU and no structural issues exist', () => {
    const course = makeCourse([makeModule('01-foo', [makeLesson('01-01-intro')])]);
    writeFsLesson('01-foo', '01-01-intro', { ru: true });
    expect(isCoverageFailing(buildCoverageReport(course, workDir))).toBe(false);
  });

  it('does not fail when EN translations are missing', () => {
    const course = makeCourse([
      makeModule('01-foo', [makeLesson('01-01-intro'), makeLesson('01-02-deep')]),
    ]);
    writeFsLesson('01-foo', '01-01-intro', { ru: true });
    writeFsLesson('01-foo', '01-02-deep', { ru: true });
    expect(isCoverageFailing(buildCoverageReport(course, workDir))).toBe(false);
  });
});
