'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import type { Course } from '@/lib/course';
import { parseDurationMin } from '@/lib/format';
import {
  getProgress,
  isCompleted,
  lessonKey,
  PROGRESS_CHANGE_EVENT,
  PROGRESS_STORAGE_KEY,
  type ProgressMap,
} from '@/lib/progress';
import styles from './ProgramDrawer.module.css';

type ProgramDrawerProps = {
  course: Course;
  currentModuleId?: string;
  currentSlug?: string;
  isOpen: boolean;
  onClose: () => void;
};

export function ProgramDrawer({
  course,
  currentModuleId,
  currentSlug,
  isOpen,
  onClose,
}: ProgramDrawerProps) {
  const [progress, setProgress] = useState<ProgressMap | null>(null);

  // Default expanded set:
  //   • the module containing the active lesson, if any (so the user lands
  //     on their own context),
  //   • otherwise the first two modules — enough to communicate the
  //     accordion shape without the drawer becoming a wall of text.
  const initialExpanded = useMemo(() => {
    const map: Record<string, boolean> = {};
    course.modules.forEach((m, i) => {
      map[m.id] = currentModuleId ? m.id === currentModuleId : i < 2;
    });
    return map;
  }, [course, currentModuleId]);

  const [expanded, setExpanded] = useState<Record<string, boolean>>(initialExpanded);

  // Re-seed expansion when the active module changes — opening the drawer
  // from a different lesson should snap to that module.
  useEffect(() => {
    setExpanded(initialExpanded);
  }, [initialExpanded]);

  useEffect(() => {
    setProgress(getProgress());
    function syncFromStorage(event: StorageEvent) {
      if (event.key !== PROGRESS_STORAGE_KEY) return;
      setProgress(getProgress());
    }
    function syncFromLocal() {
      setProgress(getProgress());
    }
    window.addEventListener('storage', syncFromStorage);
    window.addEventListener(PROGRESS_CHANGE_EVENT, syncFromLocal);
    return () => {
      window.removeEventListener('storage', syncFromStorage);
      window.removeEventListener(PROGRESS_CHANGE_EVENT, syncFromLocal);
    };
  }, []);

  useEffect(() => {
    if (!isOpen) return;
    function handleKey(event: KeyboardEvent) {
      if (event.key === 'Escape') onClose();
    }
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [isOpen, onClose]);

  // Lock the page behind the drawer while it's open (matches the referenced
  // prototype's body.style.overflow = 'hidden' behavior).
  useEffect(() => {
    if (!isOpen) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = prev;
    };
  }, [isOpen]);

  const toggle = (id: string) =>
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));

  return (
    <>
      <div
        className={styles.overlay}
        data-open={isOpen ? 'true' : 'false'}
        onClick={onClose}
        aria-hidden="true"
      />
      <aside
        className={styles.drawer}
        data-open={isOpen ? 'true' : 'false'}
        aria-label="Программа курса"
        aria-hidden={!isOpen}
        role="dialog"
        aria-modal="true"
      >
        <header className={styles.header}>
          <div>
            <div className={styles.eyebrow}>/ contents</div>
            <h2 className={styles.title}>Программа курса</h2>
          </div>
          <button
            type="button"
            className={styles.close}
            onClick={onClose}
            aria-label="Закрыть"
            tabIndex={isOpen ? 0 : -1}
          >
            <CloseIcon />
          </button>
        </header>

        <nav className={styles.body} aria-label="Список модулей и уроков">
          <ol className={styles.modules}>
            {course.modules.map((mod, mIndex) => {
              const total = mod.lessons.length;
              const doneCount =
                progress === null
                  ? 0
                  : mod.lessons.filter((l) =>
                      isCompleted(progress, lessonKey(mod.id, l.slug)),
                    ).length;
              const isComplete = doneCount === total && total > 0;
              const isOpenModule = !!expanded[mod.id];
              return (
                <li key={mod.id} className={styles.module}>
                  <button
                    type="button"
                    className={styles.moduleHead}
                    onClick={() => toggle(mod.id)}
                    aria-expanded={isOpenModule}
                    tabIndex={isOpen ? 0 : -1}
                  >
                    <span className={styles.moduleNum}>
                      {String(mIndex + 1).padStart(2, '0')}
                    </span>
                    <span className={styles.moduleTitle}>{mod.title}</span>
                    <span
                      className={styles.moduleBadge}
                      data-complete={isComplete ? 'true' : 'false'}
                    >
                      {doneCount}/{total}
                    </span>
                    <span className={styles.moduleChevron} aria-hidden="true">
                      {isOpenModule ? '−' : '+'}
                    </span>
                  </button>
                  {isOpenModule && (
                    <ol className={styles.lessons}>
                      {mod.lessons.map((lesson, lIndex) => {
                        const key = lessonKey(mod.id, lesson.slug);
                        const done =
                          progress !== null && isCompleted(progress, key);
                        const isCurrent =
                          mod.id === currentModuleId && lesson.slug === currentSlug;
                        const durMin = parseDurationMin(lesson.duration);
                        return (
                          <li key={lesson.slug} className={styles.lesson}>
                            <Link
                              href={`/${mod.id}/${lesson.slug}`}
                              className={styles.lessonLink}
                              aria-current={isCurrent ? 'page' : undefined}
                              data-completed={done ? 'true' : 'false'}
                              data-current={isCurrent ? 'true' : 'false'}
                              onClick={onClose}
                              tabIndex={isOpen ? 0 : -1}
                            >
                              <span className={styles.lessonNum}>
                                {String(lIndex + 1).padStart(2, '0')}
                              </span>
                              <span className={styles.lessonTitle}>
                                {lesson.title}
                              </span>
                              <span className={styles.lessonMeta} aria-hidden="true">
                                {done ? (
                                  <span className={styles.lessonCheck}>✓</span>
                                ) : (
                                  `${durMin || lesson.duration}м`
                                )}
                              </span>
                            </Link>
                          </li>
                        );
                      })}
                    </ol>
                  )}
                </li>
              );
            })}
          </ol>
        </nav>
      </aside>
    </>
  );
}

function CloseIcon() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M6 6l12 12M18 6 6 18" />
    </svg>
  );
}
