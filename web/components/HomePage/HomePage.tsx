'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
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
import {
  getCompletedCount,
  getCompletedPercent,
  getProgress,
  isCompleted,
  lessonKey,
  PROGRESS_CHANGE_EVENT,
  PROGRESS_STORAGE_KEY,
  type ProgressMap,
} from '@/lib/progress';
import styles from './HomePage.module.css';

type HomePageProps = {
  course: Course;
  level: string;
};

export function HomePage({ course, level }: HomePageProps) {
  const [progress, setProgress] = useState<ProgressMap | null>(null);

  useEffect(() => {
    setProgress(getProgress());
    function syncStorage(event: StorageEvent) {
      if (event.key !== PROGRESS_STORAGE_KEY) return;
      setProgress(getProgress());
    }
    function syncLocal() {
      setProgress(getProgress());
    }
    window.addEventListener('storage', syncStorage);
    window.addEventListener(PROGRESS_CHANGE_EVENT, syncLocal);
    return () => {
      window.removeEventListener('storage', syncStorage);
      window.removeEventListener(PROGRESS_CHANGE_EVENT, syncLocal);
    };
  }, []);

  const totalLessons = getTotalLessons(course);
  const totalDurationMin = useMemo(
    () =>
      course.modules.reduce(
        (sum, mod) =>
          sum + mod.lessons.reduce((s, l) => s + parseDurationMin(l.duration), 0),
        0,
      ),
    [course],
  );

  // Treat pre-hydration as "no progress" for the visible state — copy will
  // hint "Начать с первого урока", numbers default to 0/total. Once
  // localStorage is read, we re-render with the real values.
  const completedCount =
    progress === null ? 0 : Math.min(getCompletedCount(progress), totalLessons);
  const percent = progress === null ? 0 : getCompletedPercent(progress, totalLessons);
  const hasProgress = completedCount > 0;

  // First lesson the user hasn't completed yet (linear walk over the
  // flattened module/lesson order). null when everything is done.
  const nextEntry = useMemo(() => {
    if (progress === null) return null;
    const flat = flattenLessons(course);
    for (const entry of flat) {
      if (!isCompleted(progress, lessonKey(entry.moduleId, entry.lesson.slug))) {
        return entry;
      }
    }
    return null;
  }, [course, progress]);

  const firstEntry = useMemo(() => flattenLessons(course)[0] ?? null, [course]);
  const continueHref = nextEntry
    ? `/${nextEntry.moduleId}/${nextEntry.lesson.slug}`
    : firstEntry
      ? `/${firstEntry.moduleId}/${firstEntry.lesson.slug}`
      : '#';
  const continueLessonNum = nextEntry ? nextEntry.index + 1 : 1;
  const nextModuleTitle = nextEntry
    ? course.modules.find((m) => m.id === nextEntry.moduleId)?.title
    : null;

  return (
    <div className={styles.page}>
      <section className={styles.hero}>
        <div>
          <h1 className={styles.heroTitle}>
            Kafka <span className={styles.heroTitleAccent}>для тех, кто</span> пишет на Go
          </h1>
          <p className={styles.heroLead}>{collapseWhitespace(course.description)}</p>
          <div className={styles.ctaRow}>
            {hasProgress ? (
              <>
                <Link href={continueHref} className={`${styles.btn} ${styles.btnPrimary}`}>
                  Продолжить · урок {continueLessonNum}
                  <span className={styles.btnArrow}>→</span>
                </Link>
                <Link
                  href={firstEntry ? `/${firstEntry.moduleId}/${firstEntry.lesson.slug}` : '#'}
                  className={`${styles.btn} ${styles.btnSecondary}`}
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
              </>
            ) : (
              <>
                <Link href={continueHref} className={`${styles.btn} ${styles.btnPrimary}`}>
                  Начать с первого урока
                  <span className={styles.btnArrow}>→</span>
                </Link>
                <button
                  type="button"
                  className={`${styles.btn} ${styles.btnSecondary}`}
                  onClick={openProgramDrawer}
                >
                  Программа курса
                </button>
              </>
            )}
          </div>
          {hasProgress && nextEntry && (
            <div className={styles.nextHint}>
              <span className={styles.nextHintArrow}>↳</span>{' '}
              <span className={styles.nextHintModule}>{nextModuleTitle}</span>
              <span className={styles.nextHintSep}> / </span>
              <span className={styles.nextHintLesson}>{nextEntry.lesson.title}</span>
            </div>
          )}
        </div>

        <aside className={styles.statsCard} aria-label="Сводка прогресса">
          <div className={styles.statsProgress}>
            <div className={styles.statsProgressRow}>
              <span className={styles.statsPct}>
                {percent}
                <span className={styles.statsPctUnit}>%</span>
              </span>
              <span className={styles.statsOf}>
                {completedCount} / {totalLessons} {pluralize(totalLessons, LESSON_FORMS)}
              </span>
            </div>
            <div className={styles.statsBar} aria-hidden="true">
              <span className={styles.statsBarFill} style={{ width: `${percent}%` }} />
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
          const completedInModule =
            progress === null
              ? 0
              : mod.lessons.filter((l) =>
                  isCompleted(progress, lessonKey(mod.id, l.slug)),
                ).length;
          const total = mod.lessons.length;
          const moduleDurationMin = mod.lessons.reduce(
            (s, l) => s + parseDurationMin(l.duration),
            0,
          );
          const modulePct =
            total === 0 ? 0 : Math.round((completedInModule / total) * 100);
          const isComplete = completedInModule === total && total > 0;
          const status = isComplete
            ? 'пройдено'
            : completedInModule > 0
              ? 'в процессе'
              : 'не начато';

          return (
            <li key={mod.id} className={styles.moduleItem}>
              <Link href={`/${mod.id}`} className={styles.moduleRow}>
                <div className={styles.moduleNum}>
                  {String(mi + 1).padStart(2, '0')}
                </div>
                <div className={styles.moduleText}>
                  <h3 className={styles.moduleTitle}>{mod.title}</h3>
                  <p className={styles.moduleDesc}>{collapseWhitespace(mod.description)}</p>
                </div>
                <div
                  className={`${styles.moduleProgress} ${isComplete ? styles.moduleProgressDone : ''}`}
                >
                  <div className={styles.mpRow}>
                    <span>{status}</span>
                    <span className={styles.mpPct}>
                      {completedInModule}/{total}
                    </span>
                  </div>
                  <div className={styles.mpBar} aria-hidden="true">
                    <span
                      className={styles.mpFill}
                      style={{ width: `${modulePct}%` }}
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
                  →
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
