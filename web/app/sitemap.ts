import type { MetadataRoute } from 'next';
import { loadCourse } from '@/lib/course-loader';
import { buildSitemap } from '@/lib/sitemap';

// Load with `'en'` so `lesson.hasTranslation` reflects the EN README — the
// sitemap uses that flag to skip /en/<m>/<l> entries for untranslated lessons.
// Titles aren't needed here, so RU vs EN resolution of strings doesn't matter.
export default function sitemap(): MetadataRoute.Sitemap {
  return buildSitemap(loadCourse('en'));
}
