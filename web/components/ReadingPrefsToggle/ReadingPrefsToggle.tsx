'use client';

import { useEffect, useId, useRef, useState, type CSSProperties } from 'react';
import { AaIcon } from '@/components/Sidebar/icons';
import { useReadingPrefs } from '@/components/ReadingPrefsProvider';
import {
  CODE_FONTS,
  PROSE_FONTS,
  type CodeFont,
  type ProseFont,
  type SizeStep,
} from '@/lib/reading-prefs';
import { useT } from '@/lib/use-i18n';
import type { UIDict } from '@/lib/i18n';
import styles from './ReadingPrefsToggle.module.css';

const PROSE_SIZE_PX: Record<SizeStep, number> = {
  0: 14,
  1: 15,
  2: 16,
  3: 18,
  4: 20,
};

const CODE_SIZE_PX: Record<SizeStep, number> = {
  0: 12,
  1: 13,
  2: 13.5,
  3: 15,
  4: 17,
};

const PROSE_FONT_LABEL_KEYS: Record<ProseFont, keyof UIDict> = {
  serif: 'readingPrefsFontSerif',
  sans: 'readingPrefsFontSans',
  lora: 'readingPrefsFontLora',
};

const CODE_FONT_LABEL_KEYS: Record<CodeFont, keyof UIDict> = {
  jetbrains: 'readingPrefsFontJetBrains',
  fira: 'readingPrefsFontFira',
  plex: 'readingPrefsFontPlex',
};

const PROSE_FONT_PREVIEW_VAR: Record<ProseFont, string> = {
  serif: 'var(--font-serif)',
  sans: 'var(--font-prose-inter)',
  lora: 'var(--font-prose-lora)',
};

const CODE_FONT_PREVIEW_VAR: Record<CodeFont, string> = {
  jetbrains: 'var(--font-mono)',
  fira: 'var(--font-code-fira)',
  plex: 'var(--font-code-plex)',
};

function stepDown(step: SizeStep): SizeStep {
  return (step > 0 ? step - 1 : 0) as SizeStep;
}

function stepUp(step: SizeStep): SizeStep {
  return (step < 4 ? step + 1 : 4) as SizeStep;
}

export function ReadingPrefsToggle() {
  const t = useT();
  const { prefs, setProseSize, setCodeSize, setProseFont, setCodeFont, reset } =
    useReadingPrefs();
  const [open, setOpen] = useState(false);
  const [renderedSizes, setRenderedSizes] = useState<{ prose: string; code: string } | null>(null);
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

  // Read the actually rendered prose/code font sizes from CSS so the labels stay
  // truthful even when a viewport-specific media query (e.g. mobile <=720px)
  // shifts the scale away from the desktop steps.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    function readActual() {
      const cs = window.getComputedStyle(document.documentElement);
      const prose = cs.getPropertyValue('--prose-font-size').trim();
      const code = cs.getPropertyValue('--code-font-size').trim();
      if (prose && code) setRenderedSizes({ prose, code });
      else setRenderedSizes(null);
    }
    readActual();
    if (typeof window.matchMedia !== 'function') return;
    const mq = window.matchMedia('(max-width: 720px)');
    mq.addEventListener('change', readActual);
    return () => mq.removeEventListener('change', readActual);
  }, [prefs.proseSize, prefs.codeSize]);

  function handleReset() {
    reset();
    setOpen(false);
  }

  const proseSizeLabel = renderedSizes?.prose ?? `${PROSE_SIZE_PX[prefs.proseSize]}px`;
  const codeSizeLabel = renderedSizes?.code ?? `${CODE_SIZE_PX[prefs.codeSize]}px`;
  const proseMin = prefs.proseSize === 0;
  const proseMax = prefs.proseSize === 4;
  const codeMin = prefs.codeSize === 0;
  const codeMax = prefs.codeSize === 4;

  return (
    <div className={styles.wrapper} ref={wrapperRef}>
      <button
        type="button"
        className={styles.trigger}
        aria-label={t.readingPrefsLabel}
        title={t.readingPrefsLabel}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-controls={popoverId}
        onClick={() => setOpen((prev) => !prev)}
      >
        <AaIcon />
      </button>
      <div
        id={popoverId}
        role="dialog"
        aria-label={t.readingPrefsLabel}
        className={styles.popover}
        data-open={open ? 'true' : 'false'}
        hidden={!open}
      >
        <section className={styles.section} aria-label={t.readingPrefsProseSection}>
          <h3 className={styles.sectionTitle}>{t.readingPrefsProseSection}</h3>

          <div className={styles.row}>
            <span className={styles.rowLabel}>{t.readingPrefsSize}</span>
            <div className={styles.sizeControl}>
              <button
                type="button"
                className={styles.sizeButton}
                aria-label={t.readingPrefsDecrease}
                disabled={proseMin}
                onClick={() => setProseSize(stepDown(prefs.proseSize))}
                data-kind="prose-decrease"
              >
                {t.readingPrefsDecrease}
              </button>
              <span className={styles.sizeValue} aria-live="polite" data-kind="prose-value">
                {proseSizeLabel}
              </span>
              <button
                type="button"
                className={styles.sizeButton}
                aria-label={t.readingPrefsIncrease}
                disabled={proseMax}
                onClick={() => setProseSize(stepUp(prefs.proseSize))}
                data-kind="prose-increase"
              >
                {t.readingPrefsIncrease}
              </button>
            </div>
          </div>

          <div className={styles.row}>
            <span className={styles.rowLabel}>{t.readingPrefsFont}</span>
            <div
              className={styles.pillGroup}
              role="radiogroup"
              aria-label={`${t.readingPrefsProseSection} — ${t.readingPrefsFont}`}
            >
              {PROSE_FONTS.map((value) => {
                const active = prefs.proseFont === value;
                const label = t[PROSE_FONT_LABEL_KEYS[value]];
                const previewStyle: CSSProperties = {
                  fontFamily: PROSE_FONT_PREVIEW_VAR[value],
                };
                return (
                  <button
                    key={value}
                    type="button"
                    role="radio"
                    aria-checked={active}
                    aria-label={label}
                    className={styles.pill}
                    data-active={active ? 'true' : 'false'}
                    data-prose-font={value}
                    onClick={() => setProseFont(value)}
                    style={previewStyle}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </div>
        </section>

        <section className={styles.section} aria-label={t.readingPrefsCodeSection}>
          <h3 className={styles.sectionTitle}>{t.readingPrefsCodeSection}</h3>

          <div className={styles.row}>
            <span className={styles.rowLabel}>{t.readingPrefsSize}</span>
            <div className={styles.sizeControl}>
              <button
                type="button"
                className={styles.sizeButton}
                aria-label={t.readingPrefsDecrease}
                disabled={codeMin}
                onClick={() => setCodeSize(stepDown(prefs.codeSize))}
                data-kind="code-decrease"
              >
                {t.readingPrefsDecrease}
              </button>
              <span className={styles.sizeValue} aria-live="polite" data-kind="code-value">
                {codeSizeLabel}
              </span>
              <button
                type="button"
                className={styles.sizeButton}
                aria-label={t.readingPrefsIncrease}
                disabled={codeMax}
                onClick={() => setCodeSize(stepUp(prefs.codeSize))}
                data-kind="code-increase"
              >
                {t.readingPrefsIncrease}
              </button>
            </div>
          </div>

          <div className={styles.row}>
            <span className={styles.rowLabel}>{t.readingPrefsFont}</span>
            <div
              className={styles.pillGroup}
              role="radiogroup"
              aria-label={`${t.readingPrefsCodeSection} — ${t.readingPrefsFont}`}
            >
              {CODE_FONTS.map((value) => {
                const active = prefs.codeFont === value;
                const label = t[CODE_FONT_LABEL_KEYS[value]];
                const previewStyle: CSSProperties = {
                  fontFamily: CODE_FONT_PREVIEW_VAR[value],
                };
                return (
                  <button
                    key={value}
                    type="button"
                    role="radio"
                    aria-checked={active}
                    aria-label={label}
                    className={styles.pill}
                    data-active={active ? 'true' : 'false'}
                    data-code-font={value}
                    onClick={() => setCodeFont(value)}
                    style={previewStyle}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </div>
        </section>

        <div className={styles.footer}>
          <button
            type="button"
            className={styles.resetButton}
            onClick={handleReset}
            data-kind="reset"
          >
            {t.readingPrefsReset}
          </button>
        </div>
      </div>
    </div>
  );
}
