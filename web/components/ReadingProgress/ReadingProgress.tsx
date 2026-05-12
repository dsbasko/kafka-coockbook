'use client';

import { useEffect, useState } from 'react';
import { useT } from '@/lib/use-i18n';
import styles from './ReadingProgress.module.css';

type ReadingProgressProps = {
  targetSelector?: string;
};

export function ReadingProgress({
  targetSelector = '[data-reading-target]',
}: ReadingProgressProps) {
  const t = useT();
  const [pct, setPct] = useState(0);

  useEffect(() => {
    const compute = () => {
      const target = document.querySelector(targetSelector) as HTMLElement | null;
      if (!target) {
        setPct(0);
        return;
      }
      const rect = target.getBoundingClientRect();
      const height = target.scrollHeight;
      const viewport = window.innerHeight;
      const scrolled = -rect.top;
      const maxScroll = Math.max(1, height - viewport);
      const ratio = Math.max(0, Math.min(1, scrolled / maxScroll));
      setPct(ratio * 100);
    };

    compute();
    window.addEventListener('scroll', compute, { passive: true });
    window.addEventListener('resize', compute);
    return () => {
      window.removeEventListener('scroll', compute);
      window.removeEventListener('resize', compute);
    };
  }, [targetSelector]);

  return (
    <div
      className={styles.bar}
      style={{ width: `${pct}%` }}
      role="progressbar"
      aria-label={t.readingProgressLabel}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(pct)}
    />
  );
}
