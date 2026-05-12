import type { ReactNode } from 'react';
import { getDict } from '@/lib/i18n';
import { type Lang } from '@/lib/lang';
import styles from './LessonPageLayout.module.css';

type LessonPageLayoutProps = {
  lang: Lang;
  title: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  tocSlot?: ReactNode;
  sideMetaSlot?: ReactNode;
};

export function LessonPageLayout({
  lang,
  title,
  children,
  footer,
  tocSlot,
  sideMetaSlot,
}: LessonPageLayoutProps) {
  const hasSide = Boolean(tocSlot || sideMetaSlot);
  const t = getDict(lang);

  return (
    <div className={styles.page} data-has-side={hasSide ? 'true' : 'false'}>
      <div className={styles.content}>
        <article className={styles.article} data-reading-target="">
          <header className={styles.hero}>
            <h1 className={styles.title}>{title}</h1>
          </header>
          <div className={styles.prose}>{children}</div>
        </article>
        {footer && <footer className={styles.footer}>{footer}</footer>}
      </div>

      {hasSide && (
        <aside className={styles.side} aria-label={t.lessonInfoLabel}>
          {tocSlot}
          {sideMetaSlot}
        </aside>
      )}
    </div>
  );
}
