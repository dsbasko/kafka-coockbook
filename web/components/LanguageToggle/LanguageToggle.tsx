'use client';

import { useEffect, useId, useRef, useState } from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { LanguageIcon } from '@/components/Sidebar/icons';
import { useT } from '@/lib/use-i18n';
import {
  type Lang,
  LANG_LABELS,
  LANGS,
  stripLangFromPath,
  writeStoredLang,
} from '@/lib/lang';
import styles from '../ThemeToggle/ThemeToggle.module.css';

function replacePath(pathname: string | null, next: Lang): string {
  const { rest } = stripLangFromPath(pathname ?? '/');
  if (rest === '/' || rest === '') return `/${next}/`;
  return `/${next}${rest}`;
}

export function LanguageToggle() {
  const t = useT();
  const router = useRouter();
  const pathname = usePathname();
  const current = stripLangFromPath(pathname ?? '/').lang;
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const popoverId = useId();

  useEffect(() => {
    if (!open) return;
    function handlePointer(event: MouseEvent) {
      const target = event.target as Node | null;
      if (target && wrapperRef.current?.contains(target)) return;
      setOpen(false);
    }
    function handleKey(event: KeyboardEvent) {
      if (event.key === 'Escape') setOpen(false);
    }
    document.addEventListener('mousedown', handlePointer);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handlePointer);
      document.removeEventListener('keydown', handleKey);
    };
  }, [open]);

  function handleSelect(next: Lang) {
    writeStoredLang(next);
    setOpen(false);
    if (next === current) return;
    router.push(replacePath(pathname, next));
  }

  return (
    <div className={styles.wrapper} ref={wrapperRef}>
      <button
        type="button"
        className={styles.trigger}
        aria-label={t.language}
        title={t.language}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={popoverId}
        onClick={() => setOpen((prev) => !prev)}
      >
        <LanguageIcon />
      </button>
      <div
        id={popoverId}
        role="menu"
        aria-label={t.language}
        className={styles.popover}
        data-open={open ? 'true' : 'false'}
        hidden={!open}
      >
        {LANGS.map((value) => {
          const active = current === value;
          return (
            <button
              key={value}
              type="button"
              role="menuitemradio"
              aria-checked={active}
              className={styles.option}
              data-active={active ? 'true' : 'false'}
              data-lang={value}
              onClick={() => handleSelect(value)}
              tabIndex={open ? 0 : -1}
            >
              <span className={styles.optionLabel}>{LANG_LABELS[value]}</span>
              <span className={styles.optionMark} aria-hidden="true">
                {active ? '✓' : ''}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}
