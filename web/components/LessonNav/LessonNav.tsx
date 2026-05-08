'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import type { FlatLessonEntry } from '@/lib/course';
import {
  getProgress,
  isCompleted,
  lessonKey,
  markCompleted,
  PROGRESS_STORAGE_KEY,
  unmarkCompleted,
} from '@/lib/progress';
import styles from './LessonNav.module.css';

type LessonNavProps = {
  moduleId: string;
  slug: string;
  prev: FlatLessonEntry | null;
  next: FlatLessonEntry | null;
};

export function LessonNav({ moduleId, slug, prev, next }: LessonNavProps) {
  const key = lessonKey(moduleId, slug);
  const [completed, setCompleted] = useState<boolean | null>(null);

  useEffect(() => {
    setCompleted(isCompleted(getProgress(), key));
    function syncFromStorage(event: StorageEvent) {
      if (event.key !== PROGRESS_STORAGE_KEY) return;
      setCompleted(isCompleted(getProgress(), key));
    }
    window.addEventListener('storage', syncFromStorage);
    return () => window.removeEventListener('storage', syncFromStorage);
  }, [key]);

  function emitChange() {
    window.dispatchEvent(new Event('kafka-cookbook:progress-change'));
  }

  function handleToggle() {
    if (completed) {
      unmarkCompleted(key);
      setCompleted(false);
    } else {
      markCompleted(key);
      setCompleted(true);
    }
    emitChange();
  }

  // Pre-hydration: render disabled placeholder so SSR HTML matches client until effect runs.
  const isReady = completed !== null;
  const buttonLabel = completed ? 'Пройдено ✓' : 'Отметить пройденным';

  return (
    <nav className={styles.nav} aria-label="Навигация по урокам">
      <div className={styles.row}>
        {prev ? (
          <Link
            href={`/${prev.moduleId}/${prev.lesson.slug}`}
            className={`${styles.link} ${styles.prev}`}
          >
            <span className={styles.label}>← Предыдущий урок</span>
            <span className={styles.title}>{prev.lesson.title}</span>
          </Link>
        ) : (
          <span className={styles.placeholder} aria-hidden="true" />
        )}
        {next ? (
          <Link
            href={`/${next.moduleId}/${next.lesson.slug}`}
            className={`${styles.link} ${styles.next}`}
          >
            <span className={styles.label}>Следующий урок →</span>
            <span className={styles.title}>{next.lesson.title}</span>
          </Link>
        ) : (
          <span className={styles.placeholder} aria-hidden="true" />
        )}
      </div>
      <div className={styles.actions}>
        <button
          type="button"
          className={styles.markButton}
          data-completed={completed ? 'true' : 'false'}
          onClick={isReady ? handleToggle : undefined}
          disabled={!isReady}
          aria-pressed={completed === true}
        >
          {buttonLabel}
        </button>
      </div>
    </nav>
  );
}
