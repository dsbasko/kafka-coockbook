'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import type { Course } from '@/lib/course';
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
          <h2 className={styles.title}>Программа курса</h2>
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
        <nav className={styles.nav} aria-label="Список модулей и уроков">
          <ol className={styles.modules}>
            {course.modules.map((mod, mIndex) => (
              <li key={mod.id} className={styles.module}>
                <div className={styles.moduleHeader}>
                  <span className={styles.moduleIndex}>
                    {String(mIndex + 1).padStart(2, '0')}
                  </span>
                  <Link
                    href={`/${mod.id}`}
                    className={styles.moduleTitle}
                    onClick={onClose}
                    tabIndex={isOpen ? 0 : -1}
                  >
                    {mod.title}
                  </Link>
                </div>
                <ol className={styles.lessons}>
                  {mod.lessons.map((lesson, lIndex) => {
                    const isCurrent =
                      mod.id === currentModuleId && lesson.slug === currentSlug;
                    const done =
                      progress !== null &&
                      isCompleted(progress, lessonKey(mod.id, lesson.slug));
                    return (
                      <li key={lesson.slug}>
                        <Link
                          href={`/${mod.id}/${lesson.slug}`}
                          className={styles.lessonLink}
                          aria-current={isCurrent ? 'page' : undefined}
                          data-completed={done ? 'true' : 'false'}
                          onClick={onClose}
                          tabIndex={isOpen ? 0 : -1}
                        >
                          <span className={styles.lessonIndex}>
                            {String(lIndex + 1).padStart(2, '0')}
                          </span>
                          <span className={styles.lessonTitle}>{lesson.title}</span>
                          <span
                            className={styles.checkSlot}
                            aria-hidden="true"
                            suppressHydrationWarning
                          >
                            {done ? <CheckIcon /> : null}
                          </span>
                        </Link>
                      </li>
                    );
                  })}
                </ol>
              </li>
            ))}
          </ol>
        </nav>
      </aside>
    </>
  );
}

function CloseIcon() {
  return (
    <svg
      width="20"
      height="20"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M6 6l12 12M18 6 6 18" />
    </svg>
  );
}

function CheckIcon() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M5 12.5 10 17 19 7" />
    </svg>
  );
}
