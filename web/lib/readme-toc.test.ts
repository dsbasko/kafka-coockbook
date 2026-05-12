import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { Course, Lesson, Module } from './course';
import { generateReadmeToc } from './readme-toc';

let workDir: string;

function makeLesson(slug: string, title: string): Lesson {
  return { slug, title, duration: '30m', tags: [], hasTranslation: false };
}

function makeModule(id: string, title: string, description: string, lessons: Lesson[]): Module {
  return { id, title, description, lessons };
}

function makeCourse(modules: Module[]): Course {
  return {
    title: 'Course',
    description: 'desc',
    basePath: '/test',
    repoUrl: 'https://example.com/test',
    modules,
  };
}

function writeEnTranslation(moduleId: string, slug: string): void {
  const enDir = path.join(workDir, moduleId, slug, 'i18n', 'en');
  mkdirSync(enDir, { recursive: true });
  writeFileSync(path.join(enDir, 'README.md'), '# en\n');
}

beforeEach(() => {
  workDir = mkdtempSync(path.join(tmpdir(), 'readme-toc-'));
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

describe('generateReadmeToc — structure', () => {
  it('renders a section per module with heading, description and bullets', () => {
    const course = makeCourse([
      makeModule('01-foundations', 'Foundations', 'Base Kafka model.', [
        makeLesson('01-01-architecture-and-kraft', 'Architecture & KRaft'),
        makeLesson('01-02-topics-and-partitions', 'Topics & Partitions'),
      ]),
    ]);
    writeEnTranslation('01-foundations', '01-01-architecture-and-kraft');
    writeEnTranslation('01-foundations', '01-02-topics-and-partitions');

    const toc = generateReadmeToc(course, { lang: 'en', lecturesRoot: workDir });
    expect(toc).toContain('### 01 — Foundations');
    expect(toc).toContain('Base Kafka model.');
    expect(toc).toContain(
      '- [01-01 — Architecture & KRaft](lectures/01-foundations/01-01-architecture-and-kraft/i18n/en/README.md)',
    );
    expect(toc).toContain(
      '- [01-02 — Topics & Partitions](lectures/01-foundations/01-02-topics-and-partitions/i18n/en/README.md)',
    );
    expect(toc.endsWith('\n')).toBe(true);
  });

  it('separates module sections with a blank line', () => {
    const course = makeCourse([
      makeModule('01-foo', 'Foo', 'foo desc', [makeLesson('01-01-intro', 'Intro')]),
      makeModule('02-bar', 'Bar', 'bar desc', [makeLesson('02-01-start', 'Start')]),
    ]);
    writeEnTranslation('01-foo', '01-01-intro');
    writeEnTranslation('02-bar', '02-01-start');

    const toc = generateReadmeToc(course, { lang: 'en', lecturesRoot: workDir });
    expect(toc).toMatch(/Intro\]\([^)]+\)\n\n### 02 — Bar/);
  });
});

describe('generateReadmeToc — EN/RU fallback', () => {
  it('links to /i18n/en/README.md when the EN file exists', () => {
    const course = makeCourse([
      makeModule('01-foo', 'Foo', 'desc', [makeLesson('01-01-intro', 'Intro')]),
    ]);
    writeEnTranslation('01-foo', '01-01-intro');
    const toc = generateReadmeToc(course, { lang: 'en', lecturesRoot: workDir });
    expect(toc).toContain('lectures/01-foo/01-01-intro/i18n/en/README.md');
    expect(toc).not.toContain('(RU only)');
  });

  it('falls back to /i18n/ru/README.md with a (RU only) marker when EN is absent', () => {
    const course = makeCourse([
      makeModule('01-foo', 'Foo', 'desc', [makeLesson('01-01-intro', 'Intro')]),
    ]);
    const toc = generateReadmeToc(course, { lang: 'en', lecturesRoot: workDir });
    expect(toc).toContain('lectures/01-foo/01-01-intro/i18n/ru/README.md) *(RU only)*');
  });

  it('always links to RU and never adds the marker for lang=ru', () => {
    const course = makeCourse([
      makeModule('01-foo', 'Foo', 'desc', [
        makeLesson('01-01-intro', 'Intro'),
        makeLesson('01-02-deep', 'Deep'),
      ]),
    ]);
    writeEnTranslation('01-foo', '01-01-intro');
    const toc = generateReadmeToc(course, { lang: 'ru', lecturesRoot: workDir });
    expect(toc).toContain('lectures/01-foo/01-01-intro/i18n/ru/README.md');
    expect(toc).toContain('lectures/01-foo/01-02-deep/i18n/ru/README.md');
    expect(toc).not.toContain('(RU only)');
  });
});

describe('generateReadmeToc — slug variants and label format', () => {
  it('handles use-case style slugs like 01-microservices-comm', () => {
    const course = makeCourse([
      makeModule('09-use-cases', 'Use cases', 'desc', [
        makeLesson('01-microservices-comm', 'Microservices Communication'),
      ]),
    ]);
    writeEnTranslation('09-use-cases', '01-microservices-comm');
    const toc = generateReadmeToc(course, { lang: 'en', lecturesRoot: workDir });
    expect(toc).toContain(
      '- [09-01 — Microservices Communication](lectures/09-use-cases/01-microservices-comm/i18n/en/README.md)',
    );
  });

  it('handles nested slugs like 01-01-architecture-and-kraft', () => {
    const course = makeCourse([
      makeModule('01-foundations', 'Foundations', 'desc', [
        makeLesson('01-01-architecture-and-kraft', 'Architecture & KRaft'),
      ]),
    ]);
    writeEnTranslation('01-foundations', '01-01-architecture-and-kraft');
    const toc = generateReadmeToc(course, { lang: 'en', lecturesRoot: workDir });
    expect(toc).toContain('- [01-01 — Architecture & KRaft]');
  });

  it('drops the numeric prefix when the slug does not match the pattern', () => {
    const course = makeCourse([
      makeModule('01-foo', 'Foo', 'desc', [makeLesson('intro', 'Intro')]),
    ]);
    const toc = generateReadmeToc(course, { lang: 'ru', lecturesRoot: workDir });
    expect(toc).toContain('- [Intro](lectures/01-foo/intro/i18n/ru/README.md)');
  });
});

describe('generateReadmeToc — options', () => {
  it('honors a custom linkPrefix', () => {
    const course = makeCourse([
      makeModule('01-foo', 'Foo', 'desc', [makeLesson('01-01-intro', 'Intro')]),
    ]);
    writeEnTranslation('01-foo', '01-01-intro');
    const toc = generateReadmeToc(course, {
      lang: 'en',
      lecturesRoot: workDir,
      linkPrefix: '../lectures',
    });
    expect(toc).toContain('](../lectures/01-foo/01-01-intro/i18n/en/README.md)');
  });

  it('uses linkPrefix as the FS probe root when lecturesRoot is omitted', () => {
    // FS layout is rooted at workDir; the FS probe path is built from `linkPrefix` when no `lecturesRoot` is given.
    // We point both at the same place to verify the probe still finds the EN translation.
    const course = makeCourse([
      makeModule('01-foo', 'Foo', 'desc', [makeLesson('01-01-intro', 'Intro')]),
    ]);
    writeEnTranslation('01-foo', '01-01-intro');
    const toc = generateReadmeToc(course, { lang: 'en', linkPrefix: workDir });
    expect(toc).toContain(`${workDir}/01-foo/01-01-intro/i18n/en/README.md`);
    expect(toc).not.toContain('(RU only)');
  });
});
