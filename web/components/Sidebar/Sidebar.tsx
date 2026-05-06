import { HomeIcon, ProgramIcon, ThemeIcon, GitHubIcon } from './icons';
import styles from './Sidebar.module.css';

const REPO_URL = 'https://github.com/dsbasko/kafka-cookbook';

export function Sidebar() {
  return (
    <aside className={styles.sidebar} aria-label="Боковая навигация">
      <div className={styles.brand} aria-hidden="true">
        <span className={styles.brandMark}>K</span>
      </div>

      <nav className={styles.nav} aria-label="Основная навигация">
        <button type="button" className={styles.button} aria-label="Главная" title="Главная">
          <HomeIcon />
        </button>
        <button
          type="button"
          className={styles.button}
          aria-label="Программа курса"
          title="Программа курса"
        >
          <ProgramIcon />
        </button>
      </nav>

      <div className={styles.footer}>
        <button
          type="button"
          className={styles.button}
          aria-label="Тема оформления"
          title="Тема оформления"
        >
          <ThemeIcon />
        </button>
        <a
          className={styles.button}
          href={REPO_URL}
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
