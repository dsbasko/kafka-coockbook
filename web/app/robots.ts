import type { MetadataRoute } from 'next';
import { loadCourse } from '@/lib/course-loader';
import { buildSiteUrl } from '@/lib/site-url';

export default function robots(): MetadataRoute.Robots {
  const course = loadCourse('ru');
  return {
    rules: [
      {
        userAgent: '*',
        allow: '/',
      },
    ],
    sitemap: `${buildSiteUrl(course.basePath)}sitemap.xml`,
  };
}
