import type { ReactNode } from 'react';
import styles from './LessonLayout.module.css';

type LessonLayoutProps = {
  children: ReactNode;
  drawerSlot?: ReactNode;
  tocSlot?: ReactNode;
};

export function LessonLayout({ children, drawerSlot, tocSlot }: LessonLayoutProps) {
  return (
    <div className={styles.layout}>
      <div className={styles.drawerSlot} aria-hidden={drawerSlot ? undefined : true}>
        {drawerSlot}
      </div>
      <div className={styles.content}>{children}</div>
      <aside className={styles.tocSlot} aria-label="Содержание">
        {tocSlot}
      </aside>
    </div>
  );
}
