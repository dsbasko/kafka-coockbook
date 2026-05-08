import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { LessonLayout } from '@/components/LessonLayout';
import { LessonMeta } from '@/components/LessonMeta';
import { LessonNav } from '@/components/LessonNav';
import { Toc } from '@/components/Toc';
import {
  findLesson,
  flattenLessons,
  getNextLesson,
  getPrevLesson,
} from '@/lib/course';
import { loadCourse } from '@/lib/course-loader';
import { getLessonContent } from '@/lib/lesson';
import { renderLessonMarkdown } from '@/lib/markdown';
import { extractDescription } from '@/lib/description';
import { buildAssetUrl, buildSiteUrl } from '@/lib/site-url';

type LessonPageProps = {
  params: { module: string; lesson: string };
};

export function generateStaticParams(): Array<{ module: string; lesson: string }> {
  const course = loadCourse();
  return flattenLessons(course).map((entry) => ({
    module: entry.moduleId,
    lesson: entry.lesson.slug,
  }));
}

export async function generateMetadata({ params }: LessonPageProps): Promise<Metadata> {
  const course = loadCourse();
  const lesson = findLesson(course, params.module, params.lesson);
  if (!lesson) {
    return { title: 'Страница не найдена · Kafka Cookbook' };
  }
  let description = course.description.replace(/\s+/g, ' ').trim();
  try {
    const { markdown } = await getLessonContent(params.module, params.lesson);
    description = extractDescription(markdown) ?? description;
  } catch {
    // metadata is best-effort; build will fail in the page render anyway
  }
  const title = `${lesson.title} · ${course.title}`;
  const url = buildSiteUrl(course.basePath, [params.module, params.lesson]);
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

export default async function LessonPage({ params }: LessonPageProps) {
  const course = loadCourse();
  const lesson = findLesson(course, params.module, params.lesson);
  if (!lesson) {
    notFound();
  }

  const { markdown } = await getLessonContent(params.module, params.lesson);
  const { content, toc } = await renderLessonMarkdown(markdown, {
    moduleId: params.module,
    slug: params.lesson,
    basePath: course.basePath,
    course,
  });

  const prev = getPrevLesson(course, params.module, params.lesson);
  const next = getNextLesson(course, params.module, params.lesson);

  return (
    <LessonLayout
      title={lesson.title}
      meta={<LessonMeta duration={lesson.duration} tags={lesson.tags} />}
      tocSlot={<Toc entries={toc} />}
    >
      <article className="markdown">{content}</article>
      <LessonNav
        moduleId={params.module}
        slug={params.lesson}
        prev={prev}
        next={next}
      />
    </LessonLayout>
  );
}
