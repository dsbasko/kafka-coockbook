import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { LessonLockedInterstitial } from '@/components/LessonLockedInterstitial';
import { LessonNav, type LessonNavLink } from '@/components/LessonNav';
import { LessonPageLayout } from '@/components/LessonPageLayout';
import { LessonSideMeta } from '@/components/LessonSideMeta';
import { ReadingProgress } from '@/components/ReadingProgress';
import { Toc } from '@/components/Toc';
import {
  findLesson,
  flattenLessons,
  getNextLesson,
  getPrevLesson,
  type Course,
  type FlatLessonEntry,
} from '@/lib/course';
import { loadCourse } from '@/lib/course-loader';
import { getLessonContent } from '@/lib/lesson';
import { renderLessonMarkdown } from '@/lib/markdown';
import { extractDescription } from '@/lib/description';
import { buildAssetUrl, buildSiteUrl, getRuntimeBasePath } from '@/lib/site-url';

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
    basePath: getRuntimeBasePath(course.basePath),
    course,
  });

  const prev = toNavLink(getPrevLesson(course, params.module, params.lesson));
  const next = toNavLink(getNextLesson(course, params.module, params.lesson));

  const currentModule = course.modules.find((m) => m.id === params.module);
  const moduleIndex = currentModule
    ? course.modules.findIndex((m) => m.id === currentModule.id) + 1
    : 0;

  // Both branches render side-by-side. CSS toggles their visibility via the
  // `data-lesson-locked` attribute on <html>, which the inline gate-init
  // script stamps synchronously before <body> is parsed (no flash) and which
  // GateProvider keeps in sync on the client (route changes, cross-tab
  // localStorage updates).
  return (
    <>
      <div data-lesson-body>
        <ReadingProgress />
        <LessonPageLayout
          title={lesson.title}
          tocSlot={<Toc entries={toc} />}
          sideMetaSlot={
            currentModule ? (
              <LessonSideMeta
                moduleId={currentModule.id}
                moduleTitle={currentModule.title}
                moduleIndex={moduleIndex}
                slug={params.lesson}
                duration={lesson.duration}
                tags={lesson.tags}
              />
            ) : null
          }
          footer={
            <LessonNav
              prev={prev}
              next={next}
              currentModuleId={params.module}
              currentSlug={params.lesson}
            />
          }
        >
          <article className="markdown">{content}</article>
        </LessonPageLayout>
      </div>
      <div data-lesson-gate>
        <LessonLockedInterstitial
          attemptedModuleId={params.module}
          attemptedSlug={params.lesson}
        />
      </div>
    </>
  );
}

function toNavLink(entry: FlatLessonEntry | null): LessonNavLink | null {
  if (!entry) return null;
  return {
    moduleId: entry.moduleId,
    slug: entry.lesson.slug,
    title: entry.lesson.title,
  };
}
