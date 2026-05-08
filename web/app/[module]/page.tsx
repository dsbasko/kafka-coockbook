import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';
import { LessonLayout } from '@/components/LessonLayout';
import { loadCourse } from '@/lib/course-loader';
import { buildAssetUrl, buildSiteUrl } from '@/lib/site-url';
import styles from './module.module.css';

type ModulePageProps = {
  params: { module: string };
};

export function generateStaticParams(): Array<{ module: string }> {
  const course = loadCourse();
  return course.modules.map((m) => ({ module: m.id }));
}

export function generateMetadata({ params }: ModulePageProps): Metadata {
  const course = loadCourse();
  const mod = course.modules.find((m) => m.id === params.module);
  if (!mod) {
    return { title: 'Страница не найдена · Kafka Cookbook' };
  }
  const description = collapseWhitespace(mod.description);
  const title = `${mod.title} · ${course.title}`;
  const url = buildSiteUrl(course.basePath, [mod.id]);
  const ogImage = {
    url: buildAssetUrl(course.basePath, '/opengraph-image'),
    width: 1200,
    height: 630,
    alt: `${course.title} — курс по Apache Kafka на Go`,
  };
  return {
    title,
    description,
    alternates: { canonical: url },
    openGraph: {
      type: 'article',
      siteName: course.title,
      title,
      description,
      url,
      locale: 'ru_RU',
      images: [ogImage],
    },
    twitter: {
      card: 'summary_large_image',
      title,
      description,
      images: [ogImage.url],
    },
  };
}

export default function ModulePage({ params }: ModulePageProps) {
  const course = loadCourse();
  const mod = course.modules.find((m) => m.id === params.module);
  if (!mod) {
    notFound();
  }

  return (
    <LessonLayout title={mod.title}>
      <p className={styles.description}>{collapseWhitespace(mod.description)}</p>
      <ol className={styles.lessons}>
        {mod.lessons.map((lesson, index) => (
          <li key={lesson.slug} className={styles.lesson}>
            <Link href={`/${mod.id}/${lesson.slug}`} className={styles.lessonLink}>
              <span className={styles.lessonIndex}>
                {String(index + 1).padStart(2, '0')}
              </span>
              <span className={styles.lessonBody}>
                <span className={styles.lessonTitle}>{lesson.title}</span>
                <span className={styles.lessonDuration}>{lesson.duration}</span>
              </span>
            </Link>
          </li>
        ))}
      </ol>
    </LessonLayout>
  );
}

function collapseWhitespace(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}
