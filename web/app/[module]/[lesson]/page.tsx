import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { LessonLayout } from '@/components/LessonLayout';
import { LessonMeta } from '@/components/LessonMeta';
import { findLesson, flattenLessons, loadCourse } from '@/lib/course';
import { getLessonContent } from '@/lib/lesson';
import { renderLessonMarkdown } from '@/lib/markdown';
import { extractDescription } from '@/lib/description';

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
  let description = course.description;
  try {
    const { markdown } = await getLessonContent(params.module, params.lesson);
    description = extractDescription(markdown) ?? description;
  } catch {
    // metadata is best-effort; build will fail in the page render anyway
  }
  return {
    title: `${lesson.title} · ${course.title}`,
    description,
  };
}

export default async function LessonPage({ params }: LessonPageProps) {
  const course = loadCourse();
  const lesson = findLesson(course, params.module, params.lesson);
  if (!lesson) {
    notFound();
  }

  const { markdown } = await getLessonContent(params.module, params.lesson);
  const { content } = await renderLessonMarkdown(markdown, {
    moduleId: params.module,
    slug: params.lesson,
    basePath: course.basePath,
  });

  return (
    <LessonLayout
      title={lesson.title}
      meta={<LessonMeta duration={lesson.duration} tags={lesson.tags} />}
    >
      <article className="markdown">{content}</article>
    </LessonLayout>
  );
}
