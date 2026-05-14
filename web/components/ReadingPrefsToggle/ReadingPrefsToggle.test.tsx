import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

const paramsRef: { current: Record<string, string | string[] | undefined> | null } = {
  current: { lang: 'ru' },
};

vi.mock('next/navigation', () => ({
  useParams: () => paramsRef.current,
}));

const setProseSize = vi.fn();
const setCodeSize = vi.fn();
const setProseFont = vi.fn();
const setCodeFont = vi.fn();
const reset = vi.fn();

const ctxRef: {
  current: {
    prefs: { proseSize: 0 | 1 | 2 | 3 | 4; codeSize: 0 | 1 | 2 | 3 | 4; proseFont: 'serif' | 'sans' | 'lora'; codeFont: 'jetbrains' | 'fira' | 'plex' };
  };
} = {
  current: {
    prefs: { proseSize: 2, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' },
  },
};

vi.mock('@/components/ReadingPrefsProvider', () => ({
  useReadingPrefs: () => ({
    prefs: ctxRef.current.prefs,
    setProseSize,
    setCodeSize,
    setProseFont,
    setCodeFont,
    reset,
  }),
}));

const { ReadingPrefsToggle } = await import('./ReadingPrefsToggle');

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  paramsRef.current = { lang: 'ru' };
  setProseSize.mockReset();
  setCodeSize.mockReset();
  setProseFont.mockReset();
  setCodeFont.mockReset();
  reset.mockReset();
  ctxRef.current = {
    prefs: { proseSize: 2, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' },
  };
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(() => {
  act(() => {
    root.unmount();
  });
  container.remove();
});

function renderToggle() {
  act(() => {
    root.render(<ReadingPrefsToggle />);
  });
}

function trigger(): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>('button[aria-haspopup="dialog"]');
  if (!node) throw new Error('toggle trigger not rendered');
  return node;
}

function popover(): HTMLDivElement {
  const node = container.querySelector<HTMLDivElement>('[role="dialog"]');
  if (!node) throw new Error('popover not rendered');
  return node;
}

function buttonByKind(kind: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-kind="${kind}"]`);
  if (!node) throw new Error(`button ${kind} not rendered`);
  return node;
}

function pillByProseFont(value: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-prose-font="${value}"]`);
  if (!node) throw new Error(`prose-font pill ${value} not rendered`);
  return node;
}

function pillByCodeFont(value: string): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-code-font="${value}"]`);
  if (!node) throw new Error(`code-font pill ${value} not rendered`);
  return node;
}

function click(node: HTMLElement) {
  act(() => {
    node.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
  });
}

function pressKey(key: string) {
  act(() => {
    document.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
  });
}

describe('ReadingPrefsToggle', () => {
  it('renders the trigger with localized aria-label and a closed popover', () => {
    renderToggle();
    expect(trigger().getAttribute('aria-expanded')).toBe('false');
    expect(trigger().getAttribute('aria-label')).toBe('Настройки шрифтов');
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('opens the popover on trigger click and closes on second click', () => {
    renderToggle();
    click(trigger());
    expect(trigger().getAttribute('aria-expanded')).toBe('true');
    expect(popover().hasAttribute('hidden')).toBe(false);
    click(trigger());
    expect(trigger().getAttribute('aria-expanded')).toBe('false');
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('closes the popover when Escape is pressed', () => {
    renderToggle();
    click(trigger());
    expect(popover().hasAttribute('hidden')).toBe(false);
    pressKey('Escape');
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('closes the popover on outside mousedown', () => {
    renderToggle();
    click(trigger());
    expect(popover().hasAttribute('hidden')).toBe(false);
    const outside = document.createElement('button');
    document.body.appendChild(outside);
    act(() => {
      outside.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
    });
    expect(popover().hasAttribute('hidden')).toBe(true);
    outside.remove();
  });

  it('renders the current prose size label in px', () => {
    ctxRef.current.prefs = { proseSize: 3, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(container.querySelector('[data-kind="prose-value"]')?.textContent).toBe('18px');
  });

  it('renders the current code size label in px', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 4, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(container.querySelector('[data-kind="code-value"]')?.textContent).toBe('17px');
  });

  it('disables A− for prose when proseSize === 0', () => {
    ctxRef.current.prefs = { proseSize: 0, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('prose-decrease').disabled).toBe(true);
    expect(buttonByKind('prose-increase').disabled).toBe(false);
  });

  it('disables A+ for prose when proseSize === 4', () => {
    ctxRef.current.prefs = { proseSize: 4, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('prose-decrease').disabled).toBe(false);
    expect(buttonByKind('prose-increase').disabled).toBe(true);
  });

  it('disables A− for code when codeSize === 0', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 0, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('code-decrease').disabled).toBe(true);
    expect(buttonByKind('code-increase').disabled).toBe(false);
  });

  it('disables A+ for code when codeSize === 4', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 4, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(buttonByKind('code-decrease').disabled).toBe(false);
    expect(buttonByKind('code-increase').disabled).toBe(true);
  });

  it('A+ for prose calls setProseSize with the next step', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    click(buttonByKind('prose-increase'));
    expect(setProseSize).toHaveBeenCalledTimes(1);
    expect(setProseSize).toHaveBeenCalledWith(3);
  });

  it('A− for prose calls setProseSize with the previous step', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 2, proseFont: 'serif', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    click(buttonByKind('prose-decrease'));
    expect(setProseSize).toHaveBeenCalledWith(1);
  });

  it('A+ for code calls setCodeSize with the next step', () => {
    renderToggle();
    click(trigger());
    click(buttonByKind('code-increase'));
    expect(setCodeSize).toHaveBeenCalledWith(3);
  });

  it('A− for code calls setCodeSize with the previous step', () => {
    renderToggle();
    click(trigger());
    click(buttonByKind('code-decrease'));
    expect(setCodeSize).toHaveBeenCalledWith(1);
  });

  it('clicking the Inter pill calls setProseFont("sans")', () => {
    renderToggle();
    click(trigger());
    click(pillByProseFont('sans'));
    expect(setProseFont).toHaveBeenCalledTimes(1);
    expect(setProseFont).toHaveBeenCalledWith('sans');
  });

  it('clicking the Lora pill calls setProseFont("lora")', () => {
    renderToggle();
    click(trigger());
    click(pillByProseFont('lora'));
    expect(setProseFont).toHaveBeenCalledWith('lora');
  });

  it('clicking the Fira Code pill calls setCodeFont("fira")', () => {
    renderToggle();
    click(trigger());
    click(pillByCodeFont('fira'));
    expect(setCodeFont).toHaveBeenCalledWith('fira');
  });

  it('clicking the IBM Plex Mono pill calls setCodeFont("plex")', () => {
    renderToggle();
    click(trigger());
    click(pillByCodeFont('plex'));
    expect(setCodeFont).toHaveBeenCalledWith('plex');
  });

  it('marks the active prose-font pill via aria-checked and data-active', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 2, proseFont: 'lora', codeFont: 'jetbrains' };
    renderToggle();
    click(trigger());
    expect(pillByProseFont('lora').getAttribute('aria-checked')).toBe('true');
    expect(pillByProseFont('lora').getAttribute('data-active')).toBe('true');
    expect(pillByProseFont('serif').getAttribute('aria-checked')).toBe('false');
  });

  it('marks the active code-font pill via aria-checked and data-active', () => {
    ctxRef.current.prefs = { proseSize: 2, codeSize: 2, proseFont: 'serif', codeFont: 'plex' };
    renderToggle();
    click(trigger());
    expect(pillByCodeFont('plex').getAttribute('aria-checked')).toBe('true');
    expect(pillByCodeFont('jetbrains').getAttribute('aria-checked')).toBe('false');
  });

  it('reset button calls reset() and closes the popover', () => {
    renderToggle();
    click(trigger());
    click(buttonByKind('reset'));
    expect(reset).toHaveBeenCalledTimes(1);
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('falls into english labels when lang param is en', () => {
    paramsRef.current = { lang: 'en' };
    renderToggle();
    expect(trigger().getAttribute('aria-label')).toBe('Reading preferences');
  });

  it('all controls become tabbable when popover is open and untabbable when closed', () => {
    renderToggle();
    expect(buttonByKind('prose-increase').getAttribute('tabindex')).toBe('-1');
    expect(pillByCodeFont('plex').getAttribute('tabindex')).toBe('-1');
    click(trigger());
    expect(buttonByKind('prose-increase').getAttribute('tabindex')).toBe('0');
    expect(pillByCodeFont('plex').getAttribute('tabindex')).toBe('0');
  });
});
