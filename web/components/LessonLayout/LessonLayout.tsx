import type { ReactNode } from 'react';
import styles from './LessonLayout.module.css';

type LessonLayoutProps = {
  children: ReactNode;
  title?: ReactNode;
  meta?: ReactNode;
  drawerSlot?: ReactNode;
  tocSlot?: ReactNode;
};

export function LessonLayout({
  children,
  title,
  meta,
  drawerSlot,
  tocSlot,
}: LessonLayoutProps) {
  return (
    <div className={styles.layout}>
      <div className={styles.drawerSlot} aria-hidden={drawerSlot ? undefined : true}>
        {drawerSlot}
      </div>
      <div className={styles.content}>
        {title && <h1 className={styles.title}>{title}</h1>}
        {meta}
        {children}
      </div>
      <aside className={styles.tocSlot} aria-label="Содержание">
        {tocSlot}
      </aside>
    </div>
  );
}
