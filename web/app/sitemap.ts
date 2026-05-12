import type { MetadataRoute } from 'next';
import { flattenLessons } from '@/lib/course';
import { loadCourse } from '@/lib/course-loader';
import { LANGS } from '@/lib/lang';
import { buildSiteUrl } from '@/lib/site-url';

// Interim sitemap — Task 17 reintroduces hreflang and the noindex filter for
// untranslated EN lessons. For now every (lang × module × lesson) triple is
// emitted so crawlers can discover the new lang-prefixed routes.
export default function sitemap(): MetadataRoute.Sitemap {
  const course = loadCourse('ru');
  const now = new Date();
  const entries: MetadataRoute.Sitemap = [];

  for (const lang of LANGS) {
    entries.push({
      url: buildSiteUrl(course.basePath, [lang]),
      lastModified: now,
      changeFrequency: 'weekly',
      priority: 1,
    });
    for (const mod of course.modules) {
      entries.push({
        url: buildSiteUrl(course.basePath, [lang, mod.id]),
        lastModified: now,
        changeFrequency: 'monthly',
        priority: 0.8,
      });
    }
    for (const entry of flattenLessons(course)) {
      entries.push({
        url: buildSiteUrl(course.basePath, [lang, entry.moduleId, entry.lesson.slug]),
        lastModified: now,
        changeFrequency: 'monthly',
        priority: 0.6,
      });
    }
  }

  return entries;
}
