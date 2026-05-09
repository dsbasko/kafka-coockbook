'use client';

import { useEffect, useState } from 'react';
import {
  getCompletedCount,
  getCompletedPercent,
  getProgress,
  PROGRESS_CHANGE_EVENT,
  PROGRESS_STORAGE_KEY,
  type ProgressMap,
} from '@/lib/progress';
import styles from './ProgressBar.module.css';

type ProgressBarProps = {
  total: number;
};

export function ProgressBar({ total }: ProgressBarProps) {
  const [map, setMap] = useState<ProgressMap | null>(null);

  useEffect(() => {
    setMap(getProgress());
    function handleStorage(event: StorageEvent) {
      if (event.key !== PROGRESS_STORAGE_KEY) return;
      setMap(getProgress());
    }
    function handleLocal() {
      setMap(getProgress());
    }
    window.addEventListener('storage', handleStorage);
    window.addEventListener(PROGRESS_CHANGE_EVENT, handleLocal);
    return () => {
      window.removeEventListener('storage', handleStorage);
      window.removeEventListener(PROGRESS_CHANGE_EVENT, handleLocal);
    };
  }, []);

  // Server / pre-hydration: render fixed-width placeholder to avoid CLS.
  if (map === null) {
    return (
      <div className={styles.bar} aria-hidden="true">
        <span className={styles.label}>— / {total}</span>
        <span className={styles.track}>
          <span className={styles.fill} style={{ width: '0%' }} />
        </span>
      </div>
    );
  }

  // Clamp to `total` so stale localStorage entries (lessons removed from
  // course.yaml after a user marked them complete) cannot push the visible
  // numerator above the denominator or break the ARIA contract
  // (`aria-valuenow` must not exceed `aria-valuemax`).
  const count = Math.min(getCompletedCount(map), total);
  const percent = getCompletedPercent(map, total);

  return (
    <div
      className={styles.bar}
      role="progressbar"
      aria-label="Прогресс прохождения курса"
      aria-valuemin={0}
      aria-valuemax={total}
      aria-valuenow={count}
      aria-valuetext={`${count} из ${total} (${percent}%)`}
    >
      <span className={styles.label}>
        {count} / {total} ({percent}%)
      </span>
      <span className={styles.track} aria-hidden="true">
        <span className={styles.fill} style={{ width: `${percent}%` }} />
      </span>
    </div>
  );
}
