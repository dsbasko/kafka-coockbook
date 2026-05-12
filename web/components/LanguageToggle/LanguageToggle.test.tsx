import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

// Opt into React's testing-environment hooks so `act` does not log a warning
// on every render. https://reactjs.org/link/wrap-tests-with-act
(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

// next/navigation is mocked before LanguageToggle imports so the component
// resolves these hooks to our stubs. useT also reads useParams via use-i18n.
const pushMock = vi.fn();
const paramsRef: { current: Record<string, string | string[] | undefined> | null } = {
  current: { lang: 'en' },
};
const pathnameRef: { current: string | null } = { current: '/en/01-foundations' };

vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: pushMock,
    replace: vi.fn(),
    prefetch: vi.fn(),
    back: vi.fn(),
    forward: vi.fn(),
    refresh: vi.fn(),
  }),
  usePathname: () => pathnameRef.current,
  useParams: () => paramsRef.current,
}));

const { LanguageToggle } = await import('./LanguageToggle');
const { LANG_STORAGE_KEY } = await import('@/lib/lang');

let container: HTMLDivElement;
let root: Root;

beforeAll(() => {
  if (typeof window.localStorage?.setItem === 'function') return;
  const store = new Map<string, string>();
  const shim: Storage = {
    get length() {
      return store.size;
    },
    clear: () => store.clear(),
    getItem: (key: string) => (store.has(key) ? store.get(key)! : null),
    setItem: (key: string, value: string) => void store.set(key, String(value)),
    removeItem: (key: string) => void store.delete(key),
    key: (index: number) => Array.from(store.keys())[index] ?? null,
  };
  Object.defineProperty(window, 'localStorage', {
    configurable: true,
    value: shim,
  });
});

beforeEach(() => {
  pushMock.mockReset();
  paramsRef.current = { lang: 'en' };
  pathnameRef.current = '/en/01-foundations';
  window.localStorage.clear();
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
    root.render(<LanguageToggle />);
  });
}

function trigger(): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>('button[aria-haspopup="menu"]');
  if (!node) throw new Error('toggle trigger not rendered');
  return node;
}

function optionFor(lang: 'ru' | 'en'): HTMLButtonElement {
  const node = container.querySelector<HTMLButtonElement>(`button[data-lang="${lang}"]`);
  if (!node) throw new Error(`option button for ${lang} not rendered`);
  return node;
}

function popover(): HTMLDivElement {
  const node = container.querySelector<HTMLDivElement>('[role="menu"]');
  if (!node) throw new Error('popover not rendered');
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

describe('LanguageToggle', () => {
  it('renders the trigger with current-language aria-label', () => {
    renderToggle();
    const button = trigger();
    expect(button.getAttribute('aria-expanded')).toBe('false');
    expect(button.getAttribute('aria-label')).toBe('Interface language');
  });

  it('keeps the popover hidden until the trigger is clicked', () => {
    renderToggle();
    expect(popover().hasAttribute('hidden')).toBe(true);
    click(trigger());
    expect(trigger().getAttribute('aria-expanded')).toBe('true');
    expect(popover().hasAttribute('hidden')).toBe(false);
  });

  it('marks the current language as active inside the popover', () => {
    renderToggle();
    click(trigger());
    expect(optionFor('en').getAttribute('aria-checked')).toBe('true');
    expect(optionFor('ru').getAttribute('aria-checked')).toBe('false');
  });

  it('rewrites the pathname when switching from en to ru and persists the choice', () => {
    pathnameRef.current = '/en/04-reliability/04-03-outbox-pattern';
    renderToggle();
    click(trigger());
    click(optionFor('ru'));
    expect(pushMock).toHaveBeenCalledTimes(1);
    expect(pushMock).toHaveBeenCalledWith('/ru/04-reliability/04-03-outbox-pattern');
    expect(window.localStorage.getItem(LANG_STORAGE_KEY)).toBe('ru');
  });

  it('rewrites the pathname when switching from ru to en', () => {
    paramsRef.current = { lang: 'ru' };
    pathnameRef.current = '/ru/01-foundations';
    renderToggle();
    click(trigger());
    click(optionFor('en'));
    expect(pushMock).toHaveBeenCalledWith('/en/01-foundations');
    expect(window.localStorage.getItem(LANG_STORAGE_KEY)).toBe('en');
  });

  it('does not navigate when selecting the already-active language but still closes the popover', () => {
    renderToggle();
    click(trigger());
    expect(popover().hasAttribute('hidden')).toBe(false);
    click(optionFor('en'));
    expect(pushMock).not.toHaveBeenCalled();
    expect(popover().hasAttribute('hidden')).toBe(true);
    expect(window.localStorage.getItem(LANG_STORAGE_KEY)).toBe('en');
  });

  it('falls back to /{lang}/ when the pathname has no lang prefix yet', () => {
    paramsRef.current = null;
    pathnameRef.current = '/';
    renderToggle();
    click(trigger());
    click(optionFor('ru'));
    expect(pushMock).toHaveBeenCalledWith('/ru/');
  });

  it('closes the popover when Escape is pressed', () => {
    renderToggle();
    click(trigger());
    expect(popover().hasAttribute('hidden')).toBe(false);
    pressKey('Escape');
    expect(trigger().getAttribute('aria-expanded')).toBe('false');
    expect(popover().hasAttribute('hidden')).toBe(true);
  });

  it('closes the popover when a mousedown lands outside the wrapper', () => {
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

  it('renders the popover label and trigger title from the active dictionary', () => {
    paramsRef.current = { lang: 'ru' };
    pathnameRef.current = '/ru/';
    renderToggle();
    expect(trigger().getAttribute('aria-label')).toBe('Язык интерфейса');
    expect(popover().getAttribute('aria-label')).toBe('Язык интерфейса');
  });

  it('switches option tabindex with popover state', () => {
    renderToggle();
    expect(optionFor('ru').getAttribute('tabindex')).toBe('-1');
    click(trigger());
    expect(optionFor('ru').getAttribute('tabindex')).toBe('0');
    expect(optionFor('en').getAttribute('tabindex')).toBe('0');
  });
});
