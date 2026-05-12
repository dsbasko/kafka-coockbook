import type { MetadataRoute } from 'next';
import { flattenLessons, type Course } from './course';
import { DEFAULT_LANG, LANGS } from './lang';
import { buildSiteUrl } from './site-url';

export interface BuildSitemapOptions {
  now?: Date;
}

type SitemapEntry = MetadataRoute.Sitemap[number];
type LanguageMap = NonNullable<NonNullable<SitemapEntry['alternates']>['languages']>;

// `lesson.hasTranslation` must reflect the EN README (load the course via
// `loadCourse('en')`). RU READMEs are guaranteed by the i18n migration, so
// every RU URL is always emitted. Missing EN lesson pages still render with
// noindex metadata — they just stay out of the sitemap.
export function buildSitemap(
  course: Course,
  options: BuildSitemapOptions = {},
): MetadataRoute.Sitemap {
  const now = options.now ?? new Date();
  const entries: MetadataRoute.Sitemap = [];

  const homeLangs = buildLangMap(course.basePath, []);
  for (const lang of LANGS) {
    entries.push({
      url: buildSiteUrl(course.basePath, [lang]),
      lastModified: now,
      changeFrequency: 'weekly',
      priority: 1,
      alternates: { languages: { ...homeLangs } },
    });
  }

  for (const mod of course.modules) {
    const moduleLangs = buildLangMap(course.basePath, [mod.id]);
    for (const lang of LANGS) {
      entries.push({
        url: buildSiteUrl(course.basePath, [lang, mod.id]),
        lastModified: now,
        changeFrequency: 'monthly',
        priority: 0.8,
        alternates: { languages: { ...moduleLangs } },
      });
    }
  }

  for (const entry of flattenLessons(course)) {
    const lessonLangs = buildLessonLangMap(
      course.basePath,
      entry.moduleId,
      entry.lesson.slug,
      entry.lesson.hasTranslation,
    );
    const ruUrl = buildSiteUrl(course.basePath, ['ru', entry.moduleId, entry.lesson.slug]);
    const enUrl = buildSiteUrl(course.basePath, ['en', entry.moduleId, entry.lesson.slug]);

    entries.push({
      url: ruUrl,
      lastModified: now,
      changeFrequency: 'monthly',
      priority: 0.6,
      alternates: { languages: { ...lessonLangs } },
    });

    if (entry.lesson.hasTranslation) {
      entries.push({
        url: enUrl,
        lastModified: now,
        changeFrequency: 'monthly',
        priority: 0.6,
        alternates: { languages: { ...lessonLangs } },
      });
    }
  }

  return entries;
}

// Build the hreflang `languages` map for a "simple" route (home `/[lang]/`
// or module `/[lang]/[module]/`) — both language URLs always exist there, so
// `x-default` points at the DEFAULT_LANG variant.
export function buildLangMap(basePath: string, tail: string[]): LanguageMap {
  const map: LanguageMap = {};
  for (const lang of LANGS) {
    map[lang] = buildSiteUrl(basePath, [lang, ...tail]);
  }
  map['x-default'] = buildSiteUrl(basePath, [DEFAULT_LANG, ...tail]);
  return map;
}

// Build the hreflang `languages` map for a lesson route. When EN README is
// missing the EN page renders a noindex fallback (RU content + banner); we
// match `buildSitemap`'s rule and omit it from `languages` so crawlers don't
// see a non-reciprocal alternate.
export function buildLessonLangMap(
  basePath: string,
  moduleId: string,
  slug: string,
  hasEn: boolean,
): LanguageMap {
  const ruUrl = buildSiteUrl(basePath, ['ru', moduleId, slug]);
  const map: LanguageMap = { ru: ruUrl };
  if (hasEn) {
    const enUrl = buildSiteUrl(basePath, ['en', moduleId, slug]);
    map.en = enUrl;
    map['x-default'] = enUrl;
  } else {
    map['x-default'] = ruUrl;
  }
  return map;
}
