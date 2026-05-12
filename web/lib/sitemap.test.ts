import { describe, expect, it } from 'vitest';
import type { Course, Lesson, Module } from './course';
import { buildSitemap } from './sitemap';

const NOW = new Date('2026-05-13T12:00:00Z');
const SITE = 'https://dsbasko.github.io';

function makeLesson(slug: string, hasTranslation: boolean): Lesson {
  return { slug, title: slug, duration: '30m', tags: [], hasTranslation };
}

function makeModule(id: string, lessons: Lesson[]): Module {
  return { id, title: id, description: `${id} desc`, lessons };
}

function makeCourse(overrides: Partial<Course> = {}): Course {
  return {
    title: 'Test',
    description: 'Test desc',
    basePath: '/test',
    repoUrl: 'https://example.com/test',
    modules: [
      makeModule('01-foo', [
        makeLesson('01-01-intro', true),
        makeLesson('01-02-deep', false),
      ]),
      makeModule('02-bar', [makeLesson('02-01-start', false)]),
    ],
    ...overrides,
  };
}

describe('buildSitemap — home pages', () => {
  it('emits one entry per language with full hreflang alternates', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    const homeRu = map.find((e) => e.url === `${SITE}/test/ru/`);
    const homeEn = map.find((e) => e.url === `${SITE}/test/en/`);
    expect(homeRu).toBeDefined();
    expect(homeEn).toBeDefined();

    const expected = {
      ru: `${SITE}/test/ru/`,
      en: `${SITE}/test/en/`,
      'x-default': `${SITE}/test/en/`,
    };
    expect(homeRu?.alternates?.languages).toEqual(expected);
    expect(homeEn?.alternates?.languages).toEqual(expected);
    expect(homeRu?.priority).toBe(1);
    expect(homeRu?.changeFrequency).toBe('weekly');
  });
});

describe('buildSitemap — module pages', () => {
  it('emits both language variants for every module regardless of translation state', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    const moduleUrls = map.map((e) => e.url).filter((url) => /\/(01-foo|02-bar)\/$/.test(url));
    expect(moduleUrls).toContain(`${SITE}/test/ru/01-foo/`);
    expect(moduleUrls).toContain(`${SITE}/test/en/01-foo/`);
    expect(moduleUrls).toContain(`${SITE}/test/ru/02-bar/`);
    expect(moduleUrls).toContain(`${SITE}/test/en/02-bar/`);
  });

  it('attaches matching hreflang alternates to module entries', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    const ruEntry = map.find((e) => e.url === `${SITE}/test/ru/01-foo/`);
    expect(ruEntry?.alternates?.languages).toEqual({
      ru: `${SITE}/test/ru/01-foo/`,
      en: `${SITE}/test/en/01-foo/`,
      'x-default': `${SITE}/test/en/01-foo/`,
    });
    expect(ruEntry?.priority).toBe(0.8);
    expect(ruEntry?.changeFrequency).toBe('monthly');
  });
});

describe('buildSitemap — lesson pages', () => {
  it('emits both /ru/ and /en/ URLs when EN translation exists', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    const urls = map.map((e) => e.url);
    expect(urls).toContain(`${SITE}/test/ru/01-foo/01-01-intro/`);
    expect(urls).toContain(`${SITE}/test/en/01-foo/01-01-intro/`);
  });

  it('emits only /ru/ URL when EN translation is missing', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    const urls = map.map((e) => e.url);
    expect(urls).toContain(`${SITE}/test/ru/01-foo/01-02-deep/`);
    expect(urls).not.toContain(`${SITE}/test/en/01-foo/01-02-deep/`);
    expect(urls).toContain(`${SITE}/test/ru/02-bar/02-01-start/`);
    expect(urls).not.toContain(`${SITE}/test/en/02-bar/02-01-start/`);
  });

  it('attaches full ru/en/x-default alternates to translated lessons (both URLs)', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    const translated = map.filter((e) => e.url.includes('/01-01-intro/'));
    expect(translated).toHaveLength(2);
    const expected = {
      ru: `${SITE}/test/ru/01-foo/01-01-intro/`,
      en: `${SITE}/test/en/01-foo/01-01-intro/`,
      'x-default': `${SITE}/test/en/01-foo/01-01-intro/`,
    };
    for (const entry of translated) {
      expect(entry.alternates?.languages).toEqual(expected);
      expect(entry.priority).toBe(0.6);
      expect(entry.changeFrequency).toBe('monthly');
    }
  });

  it('attaches ru + x-default→ru alternates to RU-only lessons without an en key', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    const entry = map.find((e) => e.url.includes('/01-02-deep/'));
    expect(entry).toBeDefined();
    expect(entry?.alternates?.languages).toEqual({
      ru: `${SITE}/test/ru/01-foo/01-02-deep/`,
      'x-default': `${SITE}/test/ru/01-foo/01-02-deep/`,
    });
    expect(entry?.alternates?.languages).not.toHaveProperty('en');
  });
});

describe('buildSitemap — totals and metadata', () => {
  it('produces the expected entry count (home + module + lesson)', () => {
    // home: 2 (one per lang), module: 2 mods × 2 langs = 4,
    // lesson: 1 translated × 2 URLs + 2 RU-only × 1 URL = 4. Total = 10.
    const map = buildSitemap(makeCourse(), { now: NOW });
    expect(map).toHaveLength(10);
  });

  it('stamps every entry with the provided lastModified', () => {
    const map = buildSitemap(makeCourse(), { now: NOW });
    expect(map.length).toBeGreaterThan(0);
    for (const entry of map) {
      expect(entry.lastModified).toEqual(NOW);
    }
  });

  it('defaults lastModified to the current Date when no override is given', () => {
    const before = Date.now();
    const map = buildSitemap(makeCourse());
    const after = Date.now();
    for (const entry of map) {
      expect(entry.lastModified).toBeInstanceOf(Date);
      const t = (entry.lastModified as Date).getTime();
      expect(t).toBeGreaterThanOrEqual(before);
      expect(t).toBeLessThanOrEqual(after);
    }
  });

  it('honors a custom basePath', () => {
    const map = buildSitemap(makeCourse({ basePath: '/custom-base' }), { now: NOW });
    expect(map.length).toBeGreaterThan(0);
    for (const entry of map) {
      expect(entry.url.startsWith(`${SITE}/custom-base/`)).toBe(true);
    }
  });

  it('returns an empty list of lesson entries when a course has no modules', () => {
    const map = buildSitemap(makeCourse({ modules: [] }), { now: NOW });
    // Only the two home entries remain.
    expect(map).toHaveLength(2);
    expect(map.every((e) => /\/(ru|en)\/$/.test(e.url))).toBe(true);
  });
});
