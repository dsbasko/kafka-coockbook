'use client';

import { useEffect, useRef, useState, type ReactNode } from 'react';
import { DEFAULT_LANG, type Lang } from '@/lib/lang';
import { getDict } from '@/lib/i18n';
import styles from './CodeBlock.module.css';

const COPY_RESET_MS = 1500;

type CodeBlockProps = {
  language: string;
  lang?: Lang;
  children: ReactNode;
};

export function CodeBlock({ language, lang = DEFAULT_LANG, children }: CodeBlockProps) {
  const t = getDict(lang);
  const figureRef = useRef<HTMLElement>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  const handleCopy = async () => {
    const code = figureRef.current?.querySelector('pre code');
    if (!code) return;
    const text = code.textContent ?? '';
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      return;
    }
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setCopied(true);
    timeoutRef.current = setTimeout(() => setCopied(false), COPY_RESET_MS);
  };

  return (
    <figure
      ref={figureRef}
      className={styles.figure}
      data-rehype-pretty-code-figure=""
      data-language={language}
    >
      <header className={styles.header}>
        <span className={styles.language}>{language}</span>
        <button
          type="button"
          className={styles.copy}
          data-copied={copied ? 'true' : 'false'}
          onClick={handleCopy}
          aria-label={copied ? t.codeBlockCopiedAriaLabel : t.codeBlockCopyAriaLabel}
        >
          {copied ? t.codeBlockCopied : t.codeBlockCopy}
        </button>
      </header>
      <div className={styles.body}>{children}</div>
    </figure>
  );
}
