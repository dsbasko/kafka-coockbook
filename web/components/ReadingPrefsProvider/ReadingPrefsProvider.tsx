'use client';

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import {
  applyPrefs,
  DEFAULT_PREFS,
  isCodeFont,
  isProseFont,
  isSizeStep,
  readStoredPrefs,
  READING_PREFS_STORAGE_KEY,
  writeStoredPrefs,
  type CodeFont,
  type ProseFont,
  type ReadingPrefs,
  type SizeStep,
} from '@/lib/reading-prefs';

type ReadingPrefsContextValue = {
  prefs: ReadingPrefs;
  setProseSize: (next: SizeStep) => void;
  setCodeSize: (next: SizeStep) => void;
  setProseFont: (next: ProseFont) => void;
  setCodeFont: (next: CodeFont) => void;
};

const ReadingPrefsContext = createContext<ReadingPrefsContextValue | null>(null);

function readFromHtml(): ReadingPrefs {
  if (typeof document === 'undefined') return { ...DEFAULT_PREFS };
  const ds = document.documentElement.dataset;
  const proseSizeRaw = ds.proseSize != null ? Number(ds.proseSize) : null;
  const codeSizeRaw = ds.codeSize != null ? Number(ds.codeSize) : null;
  return {
    proseSize: isSizeStep(proseSizeRaw) ? proseSizeRaw : DEFAULT_PREFS.proseSize,
    codeSize: isSizeStep(codeSizeRaw) ? codeSizeRaw : DEFAULT_PREFS.codeSize,
    proseFont: isProseFont(ds.proseFont) ? ds.proseFont : DEFAULT_PREFS.proseFont,
    codeFont: isCodeFont(ds.codeFont) ? ds.codeFont : DEFAULT_PREFS.codeFont,
  };
}

export function ReadingPrefsProvider({ children }: { children: ReactNode }) {
  const [prefs, setPrefs] = useState<ReadingPrefs>(DEFAULT_PREFS);
  const prefsRef = useRef<ReadingPrefs>(DEFAULT_PREFS);

  useEffect(() => {
    const next = readFromHtml();
    prefsRef.current = next;
    setPrefs(next);
  }, []);

  useEffect(() => {
    function handleStorage(event: StorageEvent) {
      if (event.key !== READING_PREFS_STORAGE_KEY) return;
      const next = readStoredPrefs();
      prefsRef.current = next;
      applyPrefs(next);
      setPrefs(next);
    }
    window.addEventListener('storage', handleStorage);
    return () => window.removeEventListener('storage', handleStorage);
  }, []);

  const updateField = useCallback(<K extends keyof ReadingPrefs>(key: K, value: ReadingPrefs[K]) => {
    const next: ReadingPrefs = { ...prefsRef.current, [key]: value };
    prefsRef.current = next;
    applyPrefs(next);
    writeStoredPrefs(next);
    setPrefs(next);
  }, []);

  const setProseSize = useCallback((next: SizeStep) => updateField('proseSize', next), [updateField]);
  const setCodeSize = useCallback((next: SizeStep) => updateField('codeSize', next), [updateField]);
  const setProseFont = useCallback((next: ProseFont) => updateField('proseFont', next), [updateField]);
  const setCodeFont = useCallback((next: CodeFont) => updateField('codeFont', next), [updateField]);

  return (
    <ReadingPrefsContext.Provider
      value={{ prefs, setProseSize, setCodeSize, setProseFont, setCodeFont }}
    >
      {children}
    </ReadingPrefsContext.Provider>
  );
}

export function useReadingPrefs(): ReadingPrefsContextValue {
  const ctx = useContext(ReadingPrefsContext);
  if (!ctx) {
    throw new Error('useReadingPrefs must be used inside <ReadingPrefsProvider>');
  }
  return ctx;
}
