'use client';

import { useEffect, useRef, useState, type ReactNode } from 'react';
import styles from './CodeBlock.module.css';

const COPY_RESET_MS = 1500;

type CodeBlockProps = {
  language: string;
  children: ReactNode;
};

export function CodeBlock({ language, children }: CodeBlockProps) {
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
          onClick={handleCopy}
          aria-label={copied ? 'Скопировано' : 'Скопировать код'}
        >
          {copied ? 'Скопировано ✓' : 'Скопировать'}
        </button>
      </header>
      <div className={styles.body}>{children}</div>
    </figure>
  );
}
