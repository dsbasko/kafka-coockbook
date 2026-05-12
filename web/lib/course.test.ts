import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  flattenLessons,
  findLesson,
  getLessonIndex,
  getNextLesson,
  getPrevLesson,
  parseCourse,
  type Course,
} from './course';
import { loadCourse } from './course-loader';

const MIN_VALID_YAML = `
title: Test Course
description: Test description
basePath: /test
repoUrl: https://example.com/test
modules:
  - id: 01-foo
    title: Foo
    description: Foo module description
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
        tags: [foo]
      - slug: 01-02-deep
        title: Deep
        duration: 45m
  - id: 02-bar
    title: Bar
    description: Bar module description
    lessons:
      - slug: 02-01-start
        title: Start
        duration: 20m
`;

const BILINGUAL_YAML = `
title: { ru: "Курс", en: "Course" }
description: { ru: "Описание", en: "Description" }
basePath: /test
repoUrl: https://example.com/test
modules:
  - id: 01-foundations
    title: { ru: "Основы", en: "Foundations" }
    description: { ru: "RU модуль", en: "EN module" }
    lessons:
      - slug: 01-01-architecture
        title: { ru: "Архитектура", en: "Architecture" }
        duration: 45m
        tags: [architecture]
      - slug: 01-02-partial
        title: { ru: "Только RU" }
        duration: 30m
  - id: 02-mixed
    title: { ru: "Микс", en: "Mix" }
    description: { ru: "RU", en: "EN" }
    lessons:
      - slug: 02-01-legacy
        title: Legacy String
        duration: 25m
`;

function loadFixture(): Course {
  return parseCourse(MIN_VALID_YAML);
}

describe('parseCourse — success (legacy string format)', () => {
  it('parses a minimal valid yaml', () => {
    const course = loadFixture();
    expect(course.title).toBe('Test Course');
    expect(course.basePath).toBe('/test');
    expect(course.repoUrl).toBe('https://example.com/test');
    expect(course.modules).toHaveLength(2);
    expect(course.modules[0].lessons).toHaveLength(2);
    expect(course.modules[0].lessons[0].tags).toEqual(['foo']);
    expect(course.modules[0].lessons[1].tags).toEqual([]);
  });

  it('defaults hasTranslation to false (loader fills it from FS)', () => {
    const course = loadFixture();
    for (const mod of course.modules) {
      for (const lesson of mod.lessons) {
        expect(lesson.hasTranslation).toBe(false);
      }
    }
  });

  it('treats legacy strings as passthrough regardless of lang', () => {
    const ru = parseCourse(MIN_VALID_YAML, 'ru');
    const en = parseCourse(MIN_VALID_YAML, 'en');
    expect(ru.title).toBe('Test Course');
    expect(en.title).toBe('Test Course');
    expect(ru.modules[0].title).toBe('Foo');
    expect(en.modules[0].title).toBe('Foo');
    expect(ru.modules[0].lessons[0].title).toBe('Intro');
    expect(en.modules[0].lessons[0].title).toBe('Intro');
  });
});

describe('parseCourse — success (bilingual LangMap format)', () => {
  it('resolves title/description for lang=ru', () => {
    const course = parseCourse(BILINGUAL_YAML, 'ru');
    expect(course.title).toBe('Курс');
    expect(course.description).toBe('Описание');
    expect(course.modules[0].title).toBe('Основы');
    expect(course.modules[0].description).toBe('RU модуль');
    expect(course.modules[0].lessons[0].title).toBe('Архитектура');
  });

  it('resolves title/description for lang=en', () => {
    const course = parseCourse(BILINGUAL_YAML, 'en');
    expect(course.title).toBe('Course');
    expect(course.description).toBe('Description');
    expect(course.modules[0].title).toBe('Foundations');
    expect(course.modules[0].description).toBe('EN module');
    expect(course.modules[0].lessons[0].title).toBe('Architecture');
  });

  it('falls back to ru when the requested lang is missing', () => {
    const course = parseCourse(BILINGUAL_YAML, 'en');
    expect(course.modules[0].lessons[1].title).toBe('Только RU');
  });

  it('allows mixing legacy strings and LangMap in the same yaml', () => {
    const ru = parseCourse(BILINGUAL_YAML, 'ru');
    const en = parseCourse(BILINGUAL_YAML, 'en');
    expect(ru.modules[1].lessons[0].title).toBe('Legacy String');
    expect(en.modules[1].lessons[0].title).toBe('Legacy String');
  });

  it('falls back to ru for an empty target locale value', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: { ru: "Основы", en: "   " }
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
`;
    const course = parseCourse(yaml, 'en');
    expect(course.modules[0].title).toBe('Основы');
  });
});

describe('parseCourse — error cases', () => {
  it('throws when title is missing', () => {
    const yaml = MIN_VALID_YAML.replace('title: Test Course\n', '');
    expect(() => parseCourse(yaml)).toThrow(/title/);
  });

  it('throws when modules is missing', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
`;
    expect(() => parseCourse(yaml)).toThrow(/modules/);
  });

  it('throws when modules is empty', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules: []
`;
    expect(() => parseCourse(yaml)).toThrow(/modules/);
  });

  it('throws when lesson has no slug', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - title: Intro
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/slug/);
  });

  it('throws on duplicate lesson slug within a module', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
      - slug: 01-01-intro
        title: Intro Again
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/duplicate lesson slug/);
  });

  it('throws on duplicate module id', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
  - id: 01-foo
    title: Foo Again
    description: D
    lessons:
      - slug: 01-02-other
        title: Other
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/duplicate module id/);
  });

  it('throws on invalid slug', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: BadSlug
        title: T
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/slug/);
  });

  it('throws on malformed YAML', () => {
    expect(() => parseCourse('title: [unclosed')).toThrow(/YAML/);
  });

  it('throws when LangMap title lacks a ru value', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: { en: "Foo" }
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
`;
    expect(() => parseCourse(yaml, 'ru')).toThrow(/\.title\.ru is required/);
  });

  it('throws when LangMap title has an empty ru value', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: { ru: "   ", en: "Foo" }
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
`;
    expect(() => parseCourse(yaml, 'en')).toThrow(/\.title\.ru is required/);
  });

  it('throws when title is an unsupported type (array)', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: [Foo, Bar]
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(
      /\.title must be a non-empty string or a \{ ru, en \} locale map/,
    );
  });
});

describe('flattenLessons', () => {
  it('preserves course.yaml order with sequential indices', () => {
    const course = loadFixture();
    const flat = flattenLessons(course);
    expect(flat).toHaveLength(3);
    expect(flat[0]).toMatchObject({ moduleId: '01-foo', index: 0 });
    expect(flat[0].lesson.slug).toBe('01-01-intro');
    expect(flat[1]).toMatchObject({ moduleId: '01-foo', index: 1 });
    expect(flat[1].lesson.slug).toBe('01-02-deep');
    expect(flat[2]).toMatchObject({ moduleId: '02-bar', index: 2 });
    expect(flat[2].lesson.slug).toBe('02-01-start');
  });
});

describe('getLessonIndex', () => {
  it('returns the linear index for a known lesson', () => {
    const course = loadFixture();
    expect(getLessonIndex(course, '01-foo', '01-01-intro')).toBe(0);
    expect(getLessonIndex(course, '01-foo', '01-02-deep')).toBe(1);
    expect(getLessonIndex(course, '02-bar', '02-01-start')).toBe(2);
  });

  it('returns -1 for unknown lessons', () => {
    const course = loadFixture();
    expect(getLessonIndex(course, '01-foo', 'no-such')).toBe(-1);
    expect(getLessonIndex(course, 'ghost', '01-01-intro')).toBe(-1);
  });
});

describe('findLesson', () => {
  it('returns the matching lesson', () => {
    const course = loadFixture();
    expect(findLesson(course, '01-foo', '01-02-deep')?.title).toBe('Deep');
  });

  it('returns null for unknown module', () => {
    const course = loadFixture();
    expect(findLesson(course, 'no-such', '01-01-intro')).toBeNull();
  });

  it('returns null for unknown lesson', () => {
    const course = loadFixture();
    expect(findLesson(course, '01-foo', 'no-such')).toBeNull();
  });
});

describe('getNextLesson / getPrevLesson', () => {
  it('returns next within the same module', () => {
    const course = loadFixture();
    const next = getNextLesson(course, '01-foo', '01-01-intro');
    expect(next?.lesson.slug).toBe('01-02-deep');
  });

  it('crosses module boundaries', () => {
    const course = loadFixture();
    const next = getNextLesson(course, '01-foo', '01-02-deep');
    expect(next?.moduleId).toBe('02-bar');
    expect(next?.lesson.slug).toBe('02-01-start');
  });

  it('returns null after the last lesson (does not wrap)', () => {
    const course = loadFixture();
    expect(getNextLesson(course, '02-bar', '02-01-start')).toBeNull();
  });

  it('returns null before the first lesson (does not wrap)', () => {
    const course = loadFixture();
    expect(getPrevLesson(course, '01-foo', '01-01-intro')).toBeNull();
  });

  it('returns prev across module boundaries', () => {
    const course = loadFixture();
    const prev = getPrevLesson(course, '02-bar', '02-01-start');
    expect(prev?.moduleId).toBe('01-foo');
    expect(prev?.lesson.slug).toBe('01-02-deep');
  });

  it('returns null for unknown lesson', () => {
    const course = loadFixture();
    expect(getNextLesson(course, '01-foo', 'no-such')).toBeNull();
    expect(getPrevLesson(course, '01-foo', 'no-such')).toBeNull();
  });
});

describe('loadCourse — translation status', () => {
  let fixtureRoot: string;
  let lecturesRoot: string;
  let coursePath: string;

  beforeAll(() => {
    fixtureRoot = mkdtempSync(path.join(tmpdir(), 'kafka-cookbook-course-'));
    lecturesRoot = path.join(fixtureRoot, 'lectures');
    coursePath = path.join(fixtureRoot, 'course.yaml');

    writeFileSync(coursePath, BILINGUAL_YAML, 'utf8');

    // Lesson 01-01: both ru + en present
    const m1l1 = path.join(lecturesRoot, '01-foundations', '01-01-architecture');
    mkdirSync(path.join(m1l1, 'i18n', 'ru'), { recursive: true });
    mkdirSync(path.join(m1l1, 'i18n', 'en'), { recursive: true });
    writeFileSync(path.join(m1l1, 'i18n', 'ru', 'README.md'), '# RU\n', 'utf8');
    writeFileSync(path.join(m1l1, 'i18n', 'en', 'README.md'), '# EN\n', 'utf8');

    // Lesson 01-02: only ru
    const m1l2 = path.join(lecturesRoot, '01-foundations', '01-02-partial');
    mkdirSync(path.join(m1l2, 'i18n', 'ru'), { recursive: true });
    writeFileSync(path.join(m1l2, 'i18n', 'ru', 'README.md'), '# RU\n', 'utf8');

    // Lesson 02-01: neither
    mkdirSync(path.join(lecturesRoot, '02-mixed', '02-01-legacy'), { recursive: true });
  });

  afterAll(() => {
    rmSync(fixtureRoot, { recursive: true, force: true });
  });

  it('marks hasTranslation=true when i18n/<lang>/README.md exists (ru)', () => {
    const course = loadCourse('ru', { filePath: coursePath, lecturesRoot });
    expect(course.modules[0].lessons[0].hasTranslation).toBe(true);
    expect(course.modules[0].lessons[1].hasTranslation).toBe(true);
    expect(course.modules[1].lessons[0].hasTranslation).toBe(false);
  });

  it('marks hasTranslation=true only for lessons with i18n/en/README.md', () => {
    const course = loadCourse('en', { filePath: coursePath, lecturesRoot });
    expect(course.modules[0].lessons[0].hasTranslation).toBe(true);
    expect(course.modules[0].lessons[1].hasTranslation).toBe(false);
    expect(course.modules[1].lessons[0].hasTranslation).toBe(false);
  });

  it('uses the requested lang to resolve LangMap fields', () => {
    const ru = loadCourse('ru', { filePath: coursePath, lecturesRoot });
    const en = loadCourse('en', { filePath: coursePath, lecturesRoot });
    expect(ru.modules[0].lessons[0].title).toBe('Архитектура');
    expect(en.modules[0].lessons[0].title).toBe('Architecture');
  });
});
