import type { ReactNode } from 'react';
import styles from './Header.module.css';

type HeaderProps = {
  breadcrumbs?: ReactNode;
  actions?: ReactNode;
};

export function Header({ breadcrumbs, actions }: HeaderProps) {
  return (
    <header className={styles.header}>
      <div className={styles.breadcrumbs} aria-label="Хлебные крошки">
        {breadcrumbs ?? <span className={styles.breadcrumbRoot}>Kafka Cookbook</span>}
      </div>
      <div className={styles.actions}>{actions}</div>
    </header>
  );
}
