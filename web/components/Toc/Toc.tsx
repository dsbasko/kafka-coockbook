'use client';

import { useEffect, useState } from 'react';
import type { TocEntry } from '@/lib/extract-toc';
import styles from './Toc.module.css';

type TocProps = {
  entries: TocEntry[];
};

const OBSERVER_OPTIONS: IntersectionObserverInit = {
  // Trigger when a heading enters the top quarter of the viewport.
  rootMargin: '-80px 0px -70% 0px',
  threshold: [0, 1],
};

export function Toc({ entries }: TocProps) {
  const [activeSlug, setActiveSlug] = useState<string | null>(
    entries.length > 0 ? entries[0].slug : null,
  );

  useEffect(() => {
    if (entries.length === 0) return;
    if (typeof window === 'undefined' || typeof IntersectionObserver === 'undefined') {
      return;
    }

    const elements = entries
      .map((entry) => document.getElementById(entry.slug))
      .filter((el): el is HTMLElement => el !== null);

    if (elements.length === 0) return;

    const visibleSlugs = new Set<string>();
    const observer = new IntersectionObserver((records) => {
      for (const r of records) {
        if (r.isIntersecting) visibleSlugs.add(r.target.id);
        else visibleSlugs.delete(r.target.id);
      }
      const firstVisible = entries.find((e) => visibleSlugs.has(e.slug));
      if (firstVisible) {
        setActiveSlug(firstVisible.slug);
      }
    }, OBSERVER_OPTIONS);

    for (const el of elements) observer.observe(el);
    return () => observer.disconnect();
  }, [entries]);

  if (entries.length === 0) return null;

  const handleClick = (slug: string) => (event: React.MouseEvent<HTMLAnchorElement>) => {
    const target = document.getElementById(slug);
    if (!target) return;
    event.preventDefault();
    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    if (typeof window !== 'undefined' && window.history?.replaceState) {
      window.history.replaceState(null, '', `#${slug}`);
    }
    setActiveSlug(slug);
  };

  return (
    <nav className={styles.toc} aria-label="Содержание">
      <p className={styles.heading}>/ contents</p>
      <ol className={styles.list}>
        {entries.map((entry) => {
          const isActive = entry.slug === activeSlug;
          return (
            <li
              key={entry.slug}
              className={styles.item}
              data-depth={entry.depth}
              data-active={isActive ? 'true' : 'false'}
            >
              <a
                href={`#${entry.slug}`}
                className={styles.link}
                onClick={handleClick(entry.slug)}
              >
                <span className={styles.marker} aria-hidden="true" />
                <span className={styles.label}>{entry.text}</span>
              </a>
            </li>
          );
        })}
      </ol>
    </nav>
  );
}
