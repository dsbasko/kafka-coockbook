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
    const segments = [entry.moduleId, entry.lesson.slug];
    const ruUrl = buildSiteUrl(course.basePath, ['ru', ...segments]);
    const enUrl = buildSiteUrl(course.basePath, ['en', ...segments]);
    const hasEn = entry.lesson.hasTranslation;

    const lessonLangs: LanguageMap = { ru: ruUrl };
    if (hasEn) {
      lessonLangs.en = enUrl;
      lessonLangs['x-default'] = enUrl;
    } else {
      lessonLangs['x-default'] = ruUrl;
    }

    entries.push({
      url: ruUrl,
      lastModified: now,
      changeFrequency: 'monthly',
      priority: 0.6,
      alternates: { languages: { ...lessonLangs } },
    });

    if (hasEn) {
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

function buildLangMap(basePath: string, tail: string[]): LanguageMap {
  const map: LanguageMap = {};
  for (const lang of LANGS) {
    map[lang] = buildSiteUrl(basePath, [lang, ...tail]);
  }
  map['x-default'] = buildSiteUrl(basePath, [DEFAULT_LANG, ...tail]);
  return map;
}
