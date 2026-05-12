import { getDict } from '@/lib/i18n';
import type { Lang } from '@/lib/lang';
import styles from './TranslationBanner.module.css';

type TranslationBannerProps = {
  lang: Lang;
};

export function TranslationBanner({ lang }: TranslationBannerProps) {
  const t = getDict(lang);
  return (
    <aside
      className={styles.banner}
      data-translation-banner
      role="note"
      aria-label={t.translationFallbackTitle}
    >
      <header className={styles.header}>
        <GlobeIcon className={styles.icon} aria-hidden="true" />
        <span className={styles.title}>{t.translationFallbackTitle}</span>
      </header>
      <p className={styles.body}>{t.translationFallbackBody}</p>
    </aside>
  );
}

function GlobeIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 16 16"
      width="16"
      height="16"
      fill="currentColor"
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <path d="M8 0a8 8 0 1 0 0 16A8 8 0 0 0 8 0Zm5.93 7.25h-2.486a13.18 13.18 0 0 0-.66-3.61 6.52 6.52 0 0 1 3.146 3.61ZM8 1.5c.78 0 1.86 1.5 2.32 4.25H5.68C6.14 3 7.22 1.5 8 1.5ZM1.55 8.75H4.05c.06 1.23.28 2.44.65 3.61A6.52 6.52 0 0 1 1.55 8.75Zm0-1.5a6.52 6.52 0 0 1 3.15-3.61c-.37 1.17-.59 2.38-.65 3.61H1.55Zm4.13 1.5h4.64C9.86 11.5 8.78 13 8 13s-1.86-1.5-2.32-3.25Zm0-1.5c.46-2.75 1.54-4.25 2.32-4.25s1.86 1.5 2.32 4.25H5.68Zm5.76 4.86c.37-1.17.59-2.38.66-3.61h2.49a6.52 6.52 0 0 1-3.15 3.61Z" />
    </svg>
  );
}
