import { getDict } from '@/lib/i18n';
import type { Lang } from '@/lib/lang';
import styles from './ProgressBar.module.css';

type ProgressBarProps = {
  total: number;
  lang: Lang;
};

/**
 * Global course progress bar in the header. Pure server-rendered shape — the
 * gate-paint inline script reads localStorage before first paint and rewrites
 * count / percent / bar-width / ARIA attributes directly via the
 * `data-progress-*` slots. No React state, no useEffect, no hydration flash.
 */
export function ProgressBar({ total, lang }: ProgressBarProps) {
  const t = getDict(lang);
  return (
    <div
      className={styles.bar}
      role="progressbar"
      aria-label={t.progressBarAriaLabel}
      aria-valuemin={0}
      aria-valuemax={total}
      aria-valuenow={0}
      aria-valuetext={`0 ${t.progressAriaConnector} ${total} (0%)`}
      data-progress-scope="global"
      data-progress-state="not-started"
      suppressHydrationWarning
    >
      <span className={styles.label}>
        <span data-progress-count suppressHydrationWarning>0</span> / {total} (
        <span data-progress-pct suppressHydrationWarning>0</span>%)
      </span>
      <span className={styles.track} aria-hidden="true">
        <span
          className={styles.fill}
          data-progress-bar
          style={{ width: '0%' }}
          suppressHydrationWarning
        />
      </span>
    </div>
  );
}
