import type { MetadataRoute } from 'next';
import { flattenLessons } from '@/lib/course';
import { loadCourse } from '@/lib/course-loader';
import { buildSiteUrl } from '@/lib/site-url';

export default function sitemap(): MetadataRoute.Sitemap {
  const course = loadCourse();
  const now = new Date();
  const entries: MetadataRoute.Sitemap = [
    {
      url: buildSiteUrl(course.basePath),
      lastModified: now,
      changeFrequency: 'weekly',
      priority: 1,
    },
  ];

  for (const mod of course.modules) {
    entries.push({
      url: buildSiteUrl(course.basePath, [mod.id]),
      lastModified: now,
      changeFrequency: 'monthly',
      priority: 0.8,
    });
  }

  for (const entry of flattenLessons(course)) {
    entries.push({
      url: buildSiteUrl(course.basePath, [entry.moduleId, entry.lesson.slug]),
      lastModified: now,
      changeFrequency: 'monthly',
      priority: 0.6,
    });
  }

  return entries;
}
