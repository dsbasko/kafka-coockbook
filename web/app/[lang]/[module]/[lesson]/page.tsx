import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { LessonLockedInterstitial } from '@/components/LessonLockedInterstitial';
import { LessonNav, type LessonNavLink } from '@/components/LessonNav';
import { LessonPageLayout } from '@/components/LessonPageLayout';
import { LessonSideMeta } from '@/components/LessonSideMeta';
import { ReadingProgress } from '@/components/ReadingProgress';
import { Toc } from '@/components/Toc';
import { TranslationBanner } from '@/components/TranslationBanner';
import {
  findLesson,
  flattenLessons,
  getNextLesson,
  getPrevLesson,
  type FlatLessonEntry,
} from '@/lib/course';
import { loadCourse } from '@/lib/course-loader';
import { isLang, LANGS, type Lang } from '@/lib/lang';
import { getLessonContent } from '@/lib/lesson';
import { renderLessonMarkdown } from '@/lib/markdown';
import { extractDescription } from '@/lib/description';
import { buildAssetUrl, buildSiteUrl, getRuntimeBasePath } from '@/lib/site-url';

type LessonPageProps = {
  params: { lang: string; module: string; lesson: string };
};

export function generateStaticParams(): Array<{
  lang: Lang;
  module: string;
  lesson: string;
}> {
  // Pages are emitted for every (lang × module × lesson) triple — even when
  // the EN translation is missing. The page renders with a fallback banner
  // and noindex metadata in that case (see generateMetadata + render below).
  const course = loadCourse('ru');
  const lessonPairs = flattenLessons(course).map((entry) => ({
    module: entry.moduleId,
    lesson: entry.lesson.slug,
  }));
  return LANGS.flatMap((lang) =>
    lessonPairs.map((pair) => ({ lang, ...pair })),
  );
}

export async function generateMetadata({
  params,
}: LessonPageProps): Promise<Metadata> {
  if (!isLang(params.lang)) return {};
  const lang = params.lang as Lang;
  const course = loadCourse(lang);
  const lesson = findLesson(course, params.module, params.lesson);
  if (!lesson) {
    return { title: 'Страница не найдена · Kafka Cookbook' };
  }

  let description = course.description.replace(/\s+/g, ' ').trim();
  let fallbackUsed = false;
  try {
    const content = await getLessonContent(params.module, params.lesson, lang);
    description = extractDescription(content.markdown) ?? description;
    fallbackUsed = content.fallbackUsed;
  } catch {
    // metadata is best-effort; the page render will surface the real failure.
  }

  const title = `${lesson.title} · ${course.title}`;
  const canonicalUrl = buildSiteUrl(course.basePath, [
    lang,
    params.module,
    params.lesson,
  ]);
  const ogImage = {
    url: buildAssetUrl(course.basePath, '/opengraph-image'),
    width: 1200,
    height: 630,
    alt: `${course.title} — курс по Apache Kafka на Go`,
  };

  // EN page falling back to RU content — withhold from search and point the
  // canonical at the actual RU URL so search engines don't index a duplicate
  // under the wrong language.
  if (lang === 'en' && fallbackUsed) {
    const ruCanonical = buildSiteUrl(course.basePath, [
      'ru',
      params.module,
      params.lesson,
    ]);
    return {
      title,
      description,
      alternates: { canonical: ruCanonical },
      robots: { index: false, follow: true },
    };
  }

  return {
    title,
    description,
    alternates: { canonical: canonicalUrl },
    openGraph: {
      type: 'article',
      siteName: course.title,
      title,
      description,
      url: canonicalUrl,
      locale: lang === 'ru' ? 'ru_RU' : 'en_US',
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
  if (!isLang(params.lang)) notFound();
  const lang = params.lang as Lang;
  const course = loadCourse(lang);
  const lesson = findLesson(course, params.module, params.lesson);
  if (!lesson) {
    notFound();
  }

  const { markdown, lang: contentLang, fallbackUsed } = await getLessonContent(
    params.module,
    params.lesson,
    lang,
  );
  const { content, toc } = await renderLessonMarkdown(markdown, {
    moduleId: params.module,
    slug: params.lesson,
    basePath: getRuntimeBasePath(course.basePath),
    course,
    // Use the language of the README we actually loaded — fallback content
    // contains RU sibling links and must be rewritten through the RU rewriter.
    lang: contentLang,
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
          <article className="markdown" data-translation-fallback={fallbackUsed ? 'true' : 'false'}>
            {lang === 'en' && fallbackUsed ? (
              <TranslationBanner lang={lang} />
            ) : null}
            {content}
          </article>
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
