'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { ThemeToggle } from '@/components/ThemeToggle';
import { HomeIcon, ProgramIcon, GitHubIcon } from './icons';
import styles from './Sidebar.module.css';

type SidebarProps = {
  onProgramClick: () => void;
  isProgramOpen: boolean;
  repoUrl: string;
};

export function Sidebar({ onProgramClick, isProgramOpen, repoUrl }: SidebarProps) {
  const pathname = usePathname() ?? '/';
  const isHome = pathname === '/';
  return (
    <aside className={styles.sidebar} aria-label="Боковая навигация">
      <nav className={styles.nav} aria-label="Основная навигация">
        <Link
          href="/"
          className={styles.button}
          aria-label="Главная"
          title="Главная"
          aria-current={isHome ? 'page' : undefined}
        >
          <HomeIcon />
        </Link>
        <button
          type="button"
          className={styles.button}
          aria-label="Программа курса"
          title="Программа курса"
          aria-haspopup="dialog"
          aria-expanded={isProgramOpen}
          onClick={onProgramClick}
        >
          <ProgramIcon />
        </button>
      </nav>

      <div className={styles.footer}>
        <ThemeToggle />
        <a
          className={styles.button}
          href={repoUrl}
          target="_blank"
          rel="noreferrer noopener"
          aria-label="Репозиторий на GitHub"
          title="Репозиторий на GitHub"
        >
          <GitHubIcon />
        </a>
      </div>
    </aside>
  );
}
