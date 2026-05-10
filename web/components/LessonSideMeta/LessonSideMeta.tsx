'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import {
  getProgress,
  isCompleted,
  lessonKey,
  PROGRESS_CHANGE_EVENT,
  PROGRESS_STORAGE_KEY,
  unmarkCompleted,
} from '@/lib/progress';
import styles from './LessonSideMeta.module.css';

type LessonSideMetaProps = {
  moduleId: string;
  moduleTitle: string;
  moduleIndex: number;
  slug: string;
  duration: string;
  tags: string[];
};

export function LessonSideMeta({
  moduleId,
  moduleTitle,
  moduleIndex,
  slug,
  duration,
  tags,
}: LessonSideMetaProps) {
  const key = lessonKey(moduleId, slug);
  const [completed, setCompleted] = useState<boolean | null>(null);

  useEffect(() => {
    setCompleted(isCompleted(getProgress(), key));
    const syncFromStorage = (event: StorageEvent) => {
      if (event.key !== PROGRESS_STORAGE_KEY) return;
      setCompleted(isCompleted(getProgress(), key));
    };
    const syncFromLocal = () => {
      setCompleted(isCompleted(getProgress(), key));
    };
    window.addEventListener('storage', syncFromStorage);
    window.addEventListener(PROGRESS_CHANGE_EVENT, syncFromLocal);
    return () => {
      window.removeEventListener('storage', syncFromStorage);
      window.removeEventListener(PROGRESS_CHANGE_EVENT, syncFromLocal);
    };
  }, [key]);

  const handleUnmark = () => {
    unmarkCompleted(key);
    setCompleted(false);
    window.dispatchEvent(new Event(PROGRESS_CHANGE_EVENT));
  };

  const moduleNum = String(moduleIndex).padStart(2, '0');

  return (
    <div className={styles.meta}>
      <div className={styles.row}>
        <span className={styles.key}>module</span>
        <Link href={`/${moduleId}`} className={styles.value}>
          {moduleNum} · {moduleTitle}
        </Link>
      </div>
      <div className={styles.row}>
        <span className={styles.key}>время чтения</span>
        <span className={styles.value}>{duration}</span>
      </div>
      {tags.length > 0 && (
        <div className={styles.row}>
          <span className={styles.key}>теги</span>
          <span className={styles.tags}>
            {tags.map((tag) => (
              <span key={tag} className={styles.tag}>
                #{tag}
              </span>
            ))}
          </span>
        </div>
      )}
      {completed && (
        <button type="button" className={styles.markButton} onClick={handleUnmark}>
          Пометить непрочитанным
        </button>
      )}
    </div>
  );
}
