'use client';

import { useMemo } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useGate } from '@/components/GateProvider';
import { LockIcon } from '@/components/ProgramDrawer/LockIcon';
import {
  flattenLessons,
  getTotalLessons,
  type Course,
} from '@/lib/course';
import {
  formatDurationHm,
  LESSON_FORMS,
  MODULE_FORMS,
  parseDurationMin,
  pluralize,
} from '@/lib/format';
import { openProgramDrawer } from '@/lib/program-drawer';
import { lessonKey } from '@/lib/progress';
import { navigateToFrontierHref } from '@/lib/frontier-link';
import styles from './HomePage.module.css';

type HomePageProps = {
  course: Course;
  level: string;
};

export function HomePage({ course, level }: HomePageProps) {
  const totalLessons = getTotalLessons(course);
  const router = useRouter();
  const { basePath } = useGate();
  const totalDurationMin = useMemo(
    () =>
      course.modules.reduce(
        (sum, mod) =>
          sum + mod.lessons.reduce((s, l) => s + parseDurationMin(l.duration), 0),
        0,
      ),
    [course],
  );

  // SSR baseline: render the "fresh user" shape — 0% progress, CTA points at
  // the first lesson, "Начать с первого урока" copy, module rows say "не
  // начато". The inline gate-paint script reads localStorage before first
  // paint and rewrites href + textContent + data-* attributes for the real
  // state, so the user with progress doesn't see this baseline flash.
  const firstEntry = useMemo(() => flattenLessons(course)[0] ?? null, [course]);
  const firstHref = firstEntry
    ? `/${firstEntry.moduleId}/${firstEntry.lesson.slug}`
    : '#';

  return (
    <div className={styles.page}>
      <section className={styles.hero}>
        <div>
          <h1 className={styles.heroTitle}>
            Kafka <span className={styles.heroTitleAccent}>для тех, кто</span> пишет на Go
          </h1>
          <p className={styles.heroLead}>{collapseWhitespace(course.description)}</p>
          <div
            className={styles.ctaRow}
            data-cta-frontier="global"
            data-cta-state="not-started"
            suppressHydrationWarning
          >
            {/* Two CTA variants: gate-paint flips data-cta-state and CSS
                shows exactly one. Pre-paint baseline = "not-started" so the
                SSR HTML reads "Начать с первого урока". */}
            <Link
              href={firstHref}
              className={`${styles.btn} ${styles.btnPrimary}`}
              data-cta-variant="not-started"
            >
              Начать с первого урока
              <span className={styles.btnArrow}>→</span>
            </Link>
            <Link
              href={firstHref}
              className={`${styles.btn} ${styles.btnPrimary}`}
              data-cta-variant="in-progress"
              data-cta-frontier-link
              suppressHydrationWarning
              onClick={(e) => navigateToFrontierHref(e, router, basePath)}
            >
              Продолжить · урок{' '}
              <span data-cta-frontier-num suppressHydrationWarning>
                1
              </span>
              <span className={styles.btnArrow}>→</span>
            </Link>
            {/* "Начать с начала" only visible in in-progress state. */}
            <Link
              href={firstHref}
              className={`${styles.btn} ${styles.btnSecondary}`}
              data-cta-variant="in-progress"
            >
              Начать с начала
            </Link>
            <button
              type="button"
              className={`${styles.btn} ${styles.btnGhost}`}
              onClick={openProgramDrawer}
            >
              Программа курса
            </button>
          </div>
          {/* Frontier hint — only visible once gate-paint marks the page as
              having progress. Lesson title is rewritten in-place. */}
          <div
            className={styles.nextHint}
            data-frontier-hint
            data-hint-state="hidden"
            suppressHydrationWarning
          >
            <span className={styles.nextHintArrow}>↳</span>{' '}
            <span
              className={styles.nextHintLesson}
              data-frontier-hint-lesson
              suppressHydrationWarning
            >
              {firstEntry?.lesson.title ?? ''}
            </span>
          </div>
        </div>

        <aside
          className={styles.statsCard}
          aria-label="Сводка прогресса"
          data-progress-scope="global"
          data-progress-state="not-started"
          suppressHydrationWarning
        >
          <div className={styles.statsProgress}>
            <div className={styles.statsProgressRow}>
              <span className={styles.statsPct}>
                <span data-progress-pct suppressHydrationWarning>
                  0
                </span>
                <span className={styles.statsPctUnit}>%</span>
              </span>
              <span className={styles.statsOf}>
                <span data-progress-count suppressHydrationWarning>
                  0
                </span>{' '}
                / {totalLessons} {pluralize(totalLessons, LESSON_FORMS)}
              </span>
            </div>
            <div className={styles.statsBar} aria-hidden="true">
              <span
                className={styles.statsBarFill}
                data-progress-bar
                style={{ width: '0%' }}
                suppressHydrationWarning
              />
            </div>
          </div>
          <dl className={styles.statsGrid}>
            <div>
              <dt className={styles.statsLabel}>Модулей</dt>
              <dd className={styles.statsValue}>{course.modules.length}</dd>
            </div>
            <div>
              <dt className={styles.statsLabel}>Уроков</dt>
              <dd className={styles.statsValue}>{totalLessons}</dd>
            </div>
            <div>
              <dt className={styles.statsLabel}>Длительность</dt>
              <dd className={styles.statsValue}>{formatDurationHm(totalDurationMin)}</dd>
            </div>
            <div>
              <dt className={styles.statsLabel}>Стек</dt>
              <dd className={styles.statsValue}>{level}</dd>
            </div>
          </dl>
        </aside>
      </section>

      <header className={styles.sectionHead}>
        <div>
          <div className={styles.sectionEyebrow}>/ contents</div>
          <h2 className={styles.sectionTitle}>Программа курса</h2>
        </div>
        <div className={styles.sectionTools}>
          {course.modules.length} {pluralize(course.modules.length, MODULE_FORMS)} ·{' '}
          {totalLessons} {pluralize(totalLessons, LESSON_FORMS)}
        </div>
      </header>

      <ol className={styles.modules}>
        {course.modules.map((mod, mi) => {
          const total = mod.lessons.length;
          const moduleDurationMin = mod.lessons.reduce(
            (s, l) => s + parseDurationMin(l.duration),
            0,
          );
          // Module is treated as a single gate target via its first lesson.
          // CSV of lesson keys feeds the gate-paint script so it can paint
          // per-module count / percent / state in this row.
          const moduleKey = mod.lessons[0]
            ? lessonKey(mod.id, mod.lessons[0].slug)
            : undefined;
          const moduleKeysCsv = mod.lessons
            .map((l) => lessonKey(mod.id, l.slug))
            .join(',');

          return (
            <li key={mod.id} className={styles.moduleItem}>
              <Link
                href={`/${mod.id}`}
                className={styles.moduleRow}
                data-lesson-key={moduleKey}
                data-progress-scope="module"
                data-progress-keys={moduleKeysCsv}
                data-progress-state="not-started"
                onClick={(e) => {
                  if (e.currentTarget.getAttribute('data-locked') === 'true') {
                    e.preventDefault();
                  }
                }}
                title="Модуль откроется после прохождения предыдущих уроков"
                suppressHydrationWarning
              >
                <div className={styles.moduleNum}>
                  {String(mi + 1).padStart(2, '0')}
                </div>
                <div className={styles.moduleText}>
                  <h3 className={styles.moduleTitle}>{mod.title}</h3>
                  <p className={styles.moduleDesc}>{collapseWhitespace(mod.description)}</p>
                </div>
                <div className={styles.moduleProgress}>
                  <div className={styles.mpRow}>
                    {/* Status label is derived from data-progress-state via
                        CSS pseudo-elements, so SSR markup is the same regardless
                        of progress. See HomePage.module.css. */}
                    <span className={styles.mpStatus} />
                    <span className={styles.mpPct}>
                      <span data-progress-count suppressHydrationWarning>
                        0
                      </span>
                      /{total}
                    </span>
                  </div>
                  <div className={styles.mpBar} aria-hidden="true">
                    <span
                      className={styles.mpFill}
                      data-progress-bar
                      style={{ width: '0%' }}
                      suppressHydrationWarning
                    />
                  </div>
                </div>
                <div className={styles.moduleMeta}>
                  <span className={styles.mmLessons}>
                    {total} {pluralize(total, LESSON_FORMS)}
                  </span>
                  <span className={styles.mmDuration}>
                    {formatDurationHm(moduleDurationMin)}
                  </span>
                </div>
                <div className={styles.arrowCell} aria-hidden="true">
                  <span className={styles.arrowOpen}>→</span>
                  <span className={styles.arrowLocked}>
                    <LockIcon />
                  </span>
                </div>
              </Link>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function collapseWhitespace(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}
