import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';
import { LessonLayout } from '@/components/LessonLayout';
import { loadCourse } from '@/lib/course';
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
  return {
    title: `${mod.title} · ${course.title}`,
    description: collapseWhitespace(mod.description),
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
