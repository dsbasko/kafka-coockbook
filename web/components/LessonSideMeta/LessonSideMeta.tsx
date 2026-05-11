'use client';

import Link from 'next/link';
import {
  lessonKey,
  PROGRESS_CHANGE_EVENT,
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

/**
 * Lesson sidebar meta. Server-rendered; the "Пометить непрочитанным" button
 * is always present in the DOM but hidden by default — the gate-paint inline
 * script stamps `data-completed` on the wrapper before first paint when the
 * lesson is in the completed set, and CSS reveals the button from there.
 * Avoids the React-driven hydration cycle that previously made the button
 * pop in after mount.
 */
export function LessonSideMeta({
  moduleId,
  moduleTitle,
  moduleIndex,
  slug,
  duration,
  tags,
}: LessonSideMetaProps) {
  const key = lessonKey(moduleId, slug);
  const moduleNum = String(moduleIndex).padStart(2, '0');

  const handleUnmark = () => {
    unmarkCompleted(key);
    // Fire the change event so GateProvider re-paints data-completed on
    // every [data-lesson-key] (including this wrapper) — the button hides
    // again via CSS without a React re-render here.
    window.dispatchEvent(new Event(PROGRESS_CHANGE_EVENT));
  };

  return (
    <div className={styles.meta} data-lesson-key={key}>
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
      <button
        type="button"
        className={styles.markButton}
        data-show-when-completed
        onClick={handleUnmark}
      >
        Пометить непрочитанным
      </button>
    </div>
  );
}
