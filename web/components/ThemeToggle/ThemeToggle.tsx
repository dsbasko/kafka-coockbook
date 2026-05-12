'use client';

import { useEffect, useId, useRef, useState } from 'react';
import { ThemeIcon } from '@/components/Sidebar/icons';
import { useTheme } from '@/components/ThemeProvider';
import { THEME_PREFERENCES, type ThemePreference } from '@/lib/theme';
import { useT } from '@/lib/use-i18n';
import styles from './ThemeToggle.module.css';

const THEME_LABEL_KEYS: Record<ThemePreference, 'themeLight' | 'themeDark' | 'themeSystem'> = {
  light: 'themeLight',
  dark: 'themeDark',
  system: 'themeSystem',
};

export function ThemeToggle() {
  const { preference, setPreference } = useTheme();
  const t = useT();
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

  function handleSelect(next: ThemePreference) {
    setPreference(next);
    setOpen(false);
  }

  return (
    <div className={styles.wrapper} ref={wrapperRef}>
      <button
        type="button"
        className={styles.trigger}
        aria-label={t.themeToggleLabel}
        title={t.themeToggleLabel}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={popoverId}
        onClick={() => setOpen((prev) => !prev)}
      >
        <ThemeIcon />
      </button>
      <div
        id={popoverId}
        role="menu"
        aria-label={t.themeToggleLabel}
        className={styles.popover}
        data-open={open ? 'true' : 'false'}
        hidden={!open}
      >
        {THEME_PREFERENCES.map((value) => {
          const active = preference === value;
          return (
            <button
              key={value}
              type="button"
              role="menuitemradio"
              aria-checked={active}
              className={styles.option}
              data-active={active ? 'true' : 'false'}
              onClick={() => handleSelect(value)}
              tabIndex={open ? 0 : -1}
            >
              <span className={styles.optionLabel}>{t[THEME_LABEL_KEYS[value]]}</span>
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
