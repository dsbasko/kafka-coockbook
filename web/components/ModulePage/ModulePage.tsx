'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import {
  type Course,
  type Module,
  flattenLessons,
} from '@/lib/course';
import {
  formatDurationHm,
  LESSON_FORMS,
  parseDurationMin,
  pluralize,
} from '@/lib/format';
import {
  getProgress,
  isCompleted,
  lessonKey,
  PROGRESS_CHANGE_EVENT,
  PROGRESS_STORAGE_KEY,
  type ProgressMap,
} from '@/lib/progress';
import styles from './ModulePage.module.css';

type ModulePageProps = {
  course: Course;
  module: Module;
  level: string;
};

export function ModulePage({ course, module, level }: ModulePageProps) {
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

  const moduleIndex = course.modules.findIndex((m) => m.id === module.id);
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
  const doneInModule =
    progress === null
      ? 0
      : module.lessons.filter((l) =>
          isCompleted(progress, lessonKey(module.id, l.slug)),
        ).length;
  const pct = totalLessons === 0 ? 0 : Math.round((doneInModule / totalLessons) * 100);
  const isComplete = totalLessons > 0 && doneInModule === totalLessons;
  const isStarted = doneInModule > 0;

  // First lesson in this module the user hasn't completed yet (linear walk).
  // Pre-hydration treats everything as not started, so this is the first lesson.
  const nextLessonInModule = useMemo(() => {
    if (progress === null) return module.lessons[0] ?? null;
    return (
      module.lessons.find(
        (l) => !isCompleted(progress, lessonKey(module.id, l.slug)),
      ) ?? null
    );
  }, [module, progress]);

  // First globally-unfinished lesson — used for the "Reread → continue elsewhere"
  // affordance when this whole module is already done.
  const globalNext = useMemo(() => {
    if (progress === null) return null;
    const flat = flattenLessons(course);
    for (const entry of flat) {
      if (!isCompleted(progress, lessonKey(entry.moduleId, entry.lesson.slug))) {
        return entry;
      }
    }
    return null;
  }, [course, progress]);

  const continueHref = nextLessonInModule
    ? `/${module.id}/${nextLessonInModule.slug}`
    : module.lessons[0]
      ? `/${module.id}/${module.lessons[0].slug}`
      : '#';

  const ctaLabel = isComplete
    ? 'Перечитать модуль'
    : isStarted && nextLessonInModule
      ? `Продолжить · ${nextLessonInModule.title}`
      : 'Начать модуль';

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

          <div className={styles.ctaRow}>
            <Link
              href={continueHref}
              className={`${styles.btn} ${isComplete ? styles.btnSecondary : styles.btnPrimary}`}
            >
              {ctaLabel}
              <span className={styles.btnArrow}>→</span>
            </Link>
            {nextModule && (
              <Link href={`/${nextModule.id}`} className={`${styles.btn} ${styles.btnGhost}`}>
                Следующий модуль <span className={styles.btnArrow}>→</span>
              </Link>
            )}
          </div>

          {isComplete && globalNext && globalNext.moduleId !== module.id && (
            <div className={styles.nextHint}>
              <span className={styles.nextHintArrow}>↳</span>{' '}
              <span className={styles.nextHintModule}>
                {course.modules.find((m) => m.id === globalNext.moduleId)?.title}
              </span>
              <span className={styles.nextHintSep}> / </span>
              <span className={styles.nextHintLesson}>{globalNext.lesson.title}</span>
            </div>
          )}
        </div>

        <aside className={styles.sideCard} aria-label="Прогресс модуля">
          <div className={styles.sideRow}>
            <span className={styles.sideLabel}>Прогресс</span>
            <span className={`${styles.sideVal} ${isComplete ? styles.sideValDone : ''}`}>
              {isComplete ? 'Пройдено ✓' : `${doneInModule} / ${totalLessons}`}
            </span>
          </div>
          <div className={styles.sideBar} aria-hidden="true">
            <span
              className={`${styles.sideFill} ${isComplete ? styles.sideFillDone : ''}`}
              style={{ width: `${pct}%` }}
            />
          </div>
          <div className={styles.sidePct}>{pct}%</div>

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

      <ol className={styles.lessons}>
        {module.lessons.map((lesson, index) => {
          const key = lessonKey(module.id, lesson.slug);
          const done = progress !== null && isCompleted(progress, key);
          const isNext = !done && lesson.slug === nextLessonInModule?.slug && !isComplete;

          const rowClasses = [
            styles.lessonRow,
            done ? styles.lessonRowDone : '',
            isNext ? styles.lessonRowNext : '',
          ]
            .filter(Boolean)
            .join(' ');

          return (
            <li key={lesson.slug} className={styles.lessonItem}>
              <Link href={`/${module.id}/${lesson.slug}`} className={rowClasses}>
                <span className={styles.lessonNum}>
                  {String(index + 1).padStart(2, '0')}
                </span>
                <span className={styles.lessonStatus} aria-hidden="true">
                  {done ? (
                    <span className={styles.lessonCheck}>✓</span>
                  ) : isNext ? (
                    <span className={styles.lessonDot} />
                  ) : (
                    <span className={styles.lessonCircle} />
                  )}
                </span>
                <span className={styles.lessonText}>
                  <span className={styles.lessonTitle}>{lesson.title}</span>
                  {isNext && (
                    <span className={styles.lessonHint}>↳ продолжить отсюда</span>
                  )}
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
