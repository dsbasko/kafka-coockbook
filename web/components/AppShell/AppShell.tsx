import type { ReactNode } from 'react';
import { Sidebar } from '@/components/Sidebar';
import { Header } from '@/components/Header';
import styles from './AppShell.module.css';

type AppShellProps = {
  children: ReactNode;
  headerBreadcrumbs?: ReactNode;
  headerActions?: ReactNode;
};

export function AppShell({ children, headerBreadcrumbs, headerActions }: AppShellProps) {
  return (
    <div className={styles.shell}>
      <Sidebar />
      <div className={styles.body}>
        <Header breadcrumbs={headerBreadcrumbs} actions={headerActions} />
        <main className={styles.main}>{children}</main>
      </div>
    </div>
  );
}
