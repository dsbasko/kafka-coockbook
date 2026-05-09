'use client';

import Link from 'next/link';
import { ThemeToggle } from '@/components/ThemeToggle';
import { HomeIcon, ProgramIcon, GitHubIcon } from './icons';
import styles from './Sidebar.module.css';

type SidebarProps = {
  onProgramClick: () => void;
  repoUrl: string;
};

export function Sidebar({ onProgramClick, repoUrl }: SidebarProps) {
  return (
    <aside className={styles.sidebar} aria-label="Боковая навигация">
      <Link href="/" className={styles.brand} aria-label="Kafka Cookbook — главная">
        <span className={styles.brandMark} aria-hidden="true">
          K
        </span>
      </Link>

      <nav className={styles.nav} aria-label="Основная навигация">
        <Link href="/" className={styles.button} aria-label="Главная" title="Главная">
          <HomeIcon />
        </Link>
        <button
          type="button"
          className={styles.button}
          aria-label="Программа курса"
          title="Программа курса"
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
