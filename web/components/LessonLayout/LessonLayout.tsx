import type { ReactNode } from 'react';
import styles from './LessonLayout.module.css';

type LessonLayoutProps = {
  children: ReactNode;
  title?: ReactNode;
};

export function LessonLayout({ children, title }: LessonLayoutProps) {
  return (
    <div className={styles.layout}>
      <div className={styles.content}>
        {title && <h1 className={styles.title}>{title}</h1>}
        {children}
      </div>
    </div>
  );
}
