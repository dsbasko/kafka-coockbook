'use client';

import { useMemo } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useGate } from '@/components/GateProvider';
import { LockIcon } from '@/components/ProgramDrawer/LockIcon';
import {
  type Course,
  type Module,
} from '@/lib/course';
import {
  formatDurationHm,
  LESSON_FORMS,
  parseDurationMin,
  pluralize,
} from '@/lib/format';
import { lessonKey } from '@/lib/progress';
import { navigateToFrontierHref } from '@/lib/frontier-link';
import styles from './ModulePage.module.css';

type ModulePageProps = {
  course: Course;
  module: Module;
  level: string;
};

export function ModulePage({ course, module, level }: ModulePageProps) {
  const moduleIndex = course.modules.findIndex((m) => m.id === module.id);
  const router = useRouter();
  const { basePath } = useGate();
  const prevModule = moduleIndex > 0 ? course.modules[moduleIndex - 1] : null;
  const nextModule =
    moduleIndex >= 0 && moduleIndex < course.modules.length - 1
      ? course.modules[moduleIndex + 1]
      : null;

  const moduleDurationMin = useMemo(
    () => module.lessons.reduce((s, l) => s + parseDurationMin(l.duration), 0),
    [module],
  );

  const totalLessons = module.lessons.length;

  // CSV of this module's lesson keys, attached to the side card and CTA row
  // so the gate-paint inline script can compute per-module progress without
  // re-deriving the module shape on its own.
  const moduleKeysCsv = useMemo(
    () => module.lessons.map((l) => lessonKey(module.id, l.slug)).join(','),
    [module],
  );

  // SSR + pre-hydration baseline: pretend nothing has been done yet. The
  // gate-paint script reads localStorage and rewrites textContent / sets
  // data-* attributes before first paint, so the user never sees this 0%
  // state flash to the real one. All React state derived from `progress`
  // has been removed from this component — it would only re-derive the
  // same numbers and cause a hydration re-render.
  const firstLesson = module.lessons[0] ?? null;
  const fallbackHref = firstLesson
    ? `/${module.id}/${firstLesson.slug}`
    : '#';

  return (
    <div className={styles.page}>
      <section className={styles.hero}>
        <div className={styles.heroText}>
          <div className={styles.eyebrow}>
            <span className={styles.eyebrowNum}>
              {String(moduleIndex + 1).padStart(2, '0')}
            </span>
            <span className={styles.eyebrowOf}>
              / {String(course.modules.length).padStart(2, '0')}
            </span>
            <span className={styles.eyebrowDot}>·</span>
            <span>
              {totalLessons} {pluralize(totalLessons, LESSON_FORMS)}
            </span>
            <span className={styles.eyebrowDot}>·</span>
            <span>{formatDurationHm(moduleDurationMin)}</span>
          </div>

          <h1 className={styles.title}>{module.title}</h1>
          <p className={styles.desc}>{collapseWhitespace(module.description)}</p>

          <div
            className={styles.ctaRow}
            data-cta-frontier="module"
            data-cta-state="not-started"
            data-progress-keys={moduleKeysCsv}
            suppressHydrationWarning
          >
            {/* Three CTA variants stacked in the DOM, exactly one visible per
                module state. The gate-paint script flips data-cta-state and
                rewrites href + title on the in-progress variant; CSS hides
                the other two. JSX never re-renders this region in response
                to progress changes — no flash. */}
            <Link
              href={fallbackHref}
              className={`${styles.btn} ${styles.btnPrimary}`}
              data-cta-variant="not-started"
            >
              Начать модуль
              <span className={styles.btnArrow}>→</span>
            </Link>
            <Link
              href={fallbackHref}
              className={`${styles.btn} ${styles.btnPrimary}`}
              data-cta-variant="in-progress"
              data-cta-frontier-link
              suppressHydrationWarning
              onClick={(e) => navigateToFrontierHref(e, router, basePath)}
            >
              Продолжить ·{' '}
              <span data-cta-frontier-title suppressHydrationWarning>
                {firstLesson?.title ?? ''}
              </span>
              <span className={styles.btnArrow}>→</span>
            </Link>
            <Link
              href={fallbackHref}
              className={`${styles.btn} ${styles.btnSecondary}`}
              data-cta-variant="complete"
            >
              Перечитать модуль
              <span className={styles.btnArrow}>→</span>
            </Link>
            {nextModule && (
              <Link href={`/${nextModule.id}`} className={`${styles.btn} ${styles.btnGhost}`}>
                Следующий модуль <span className={styles.btnArrow}>→</span>
              </Link>
            )}
          </div>
        </div>

        <aside
          className={styles.sideCard}
          aria-label="Прогресс модуля"
          data-progress-scope="module"
          data-progress-keys={moduleKeysCsv}
          data-progress-state="not-started"
          suppressHydrationWarning
        >
          <div className={styles.sideRow}>
            <span className={styles.sideLabel}>Прогресс</span>
            <span className={styles.sideVal}>
              <span data-progress-count suppressHydrationWarning>
                0
              </span>{' '}
              / {totalLessons}
            </span>
          </div>
          <div className={styles.sideBar} aria-hidden="true">
            <span
              className={styles.sideFill}
              data-progress-bar
              style={{ width: '0%' }}
              suppressHydrationWarning
            />
          </div>
          <div className={styles.sidePct}>
            <span data-progress-pct suppressHydrationWarning>
              0
            </span>
            %
          </div>

          <div className={styles.sideDivider} />

          <dl className={styles.sideMeta}>
            <div>
              <dt className={styles.sideMetaLabel}>Уроков</dt>
              <dd className={styles.sideMetaValue}>{totalLessons}</dd>
            </div>
            <div>
              <dt className={styles.sideMetaLabel}>Длительность</dt>
              <dd className={styles.sideMetaValue}>{formatDurationHm(moduleDurationMin)}</dd>
            </div>
            <div>
              <dt className={styles.sideMetaLabel}>Стек</dt>
              <dd className={styles.sideMetaValue}>{level}</dd>
            </div>
          </dl>
        </aside>
      </section>

      <header className={styles.sectionHead}>
        <div>
          <div className={styles.sectionEyebrow}>/ lessons</div>
          <h2 className={styles.sectionTitle}>Уроки модуля</h2>
        </div>
        <div className={styles.sectionTools}>
          {totalLessons} {pluralize(totalLessons, LESSON_FORMS)} ·{' '}
          {formatDurationHm(moduleDurationMin)}
        </div>
      </header>

      <ol className={styles.lessons} data-lesson-group={module.id}>
        {module.lessons.map((lesson, index) => {
          const key = lessonKey(module.id, lesson.slug);

          return (
            <li key={lesson.slug} className={styles.lessonItem}>
              <Link
                href={`/${module.id}/${lesson.slug}`}
                className={styles.lessonRow}
                data-lesson-key={key}
                onClick={(e) => {
                  if (e.currentTarget.getAttribute('data-locked') === 'true') {
                    e.preventDefault();
                  }
                }}
                title="Урок откроется после прохождения предыдущих"
              >
                <span className={styles.lessonNum}>
                  {String(index + 1).padStart(2, '0')}
                </span>
                {/* All four status glyphs are present in the DOM at all times.
                    CSS shows exactly one based on data-completed / data-next /
                    data-locked, which the gate-paint script flips synchronously
                    before paint and again on progress changes. */}
                <span className={styles.lessonStatus} aria-hidden="true">
                  <span
                    className={`${styles.lessonCircle} ${styles.statusDefault}`}
                  />
                  <span className={`${styles.lessonDot} ${styles.statusNext}`} />
                  <span className={`${styles.lessonCheck} ${styles.statusDone}`}>
                    ✓
                  </span>
                  <span
                    className={`${styles.lessonLockSlot} ${styles.statusLocked}`}
                  >
                    <LockIcon />
                  </span>
                </span>
                <span className={styles.lessonText}>
                  <span className={styles.lessonTitle}>{lesson.title}</span>
                  {/* Hint is always present in DOM; CSS shows it only when
                      the row carries data-next and not data-locked. */}
                  <span className={styles.lessonHint}>↳ продолжить отсюда</span>
                </span>
                {lesson.tags && lesson.tags.length > 0 && (
                  <span className={styles.lessonTags}>
                    {lesson.tags.slice(0, 3).map((tag) => (
                      <span key={tag} className={styles.lessonTag}>
                        #{tag}
                      </span>
                    ))}
                  </span>
                )}
                <span className={styles.lessonDuration}>
                  {parseDurationMin(lesson.duration)}
                  <span className={styles.lessonDurUnit}> м</span>
                </span>
                <span className={styles.lessonArrow} aria-hidden="true">
                  →
                </span>
              </Link>
            </li>
          );
        })}
      </ol>

      <nav className={styles.moduleNav} aria-label="Соседние модули">
        {prevModule ? (
          <Link
            href={`/${prevModule.id}`}
            className={`${styles.navCard} ${styles.navCardPrev}`}
          >
            <span className={styles.navLabel}>← Предыдущий модуль</span>
            <span className={styles.navTitle}>{prevModule.title}</span>
          </Link>
        ) : (
          <span className={`${styles.navCard} ${styles.navCardDisabled}`} aria-hidden="true" />
        )}
        {nextModule ? (
          <Link
            href={`/${nextModule.id}`}
            className={`${styles.navCard} ${styles.navCardNext}`}
          >
            <span className={styles.navLabel}>Следующий модуль →</span>
            <span className={styles.navTitle}>{nextModule.title}</span>
          </Link>
        ) : (
          <span className={`${styles.navCard} ${styles.navCardDisabled}`} aria-hidden="true" />
        )}
      </nav>
    </div>
  );
}

function collapseWhitespace(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}
