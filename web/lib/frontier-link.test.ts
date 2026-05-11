import { describe, expect, it, vi } from 'vitest';
import { navigateToFrontierHref } from './frontier-link';

type MockEventOptions = {
  href?: string | null;
  button?: number;
  metaKey?: boolean;
  ctrlKey?: boolean;
  shiftKey?: boolean;
  altKey?: boolean;
  defaultPrevented?: boolean;
};

function makeEvent(opts: MockEventOptions = {}) {
  const preventDefault = vi.fn();
  return {
    event: {
      defaultPrevented: opts.defaultPrevented ?? false,
      button: opts.button ?? 0,
      metaKey: opts.metaKey ?? false,
      ctrlKey: opts.ctrlKey ?? false,
      shiftKey: opts.shiftKey ?? false,
      altKey: opts.altKey ?? false,
      preventDefault,
      currentTarget: {
        getAttribute: (name: string) => (name === 'href' ? opts.href ?? null : null),
      },
    } as unknown as Parameters<typeof navigateToFrontierHref>[0],
    preventDefault,
  };
}

describe('navigateToFrontierHref', () => {
  it('routes to DOM href stripped of basePath on primary click', () => {
    const { event, preventDefault } = makeEvent({
      href: '/kafka-cookbook/01-foundations/01-01-architecture-and-kraft/',
    });
    const push = vi.fn();
    navigateToFrontierHref(event, { push }, '/kafka-cookbook');
    expect(preventDefault).toHaveBeenCalledOnce();
    expect(push).toHaveBeenCalledWith('/01-foundations/01-01-architecture-and-kraft/');
  });

  it('passes href through when basePath is empty', () => {
    const { event, preventDefault } = makeEvent({
      href: '/01-foundations/01-01-architecture-and-kraft/',
    });
    const push = vi.fn();
    navigateToFrontierHref(event, { push }, '');
    expect(preventDefault).toHaveBeenCalledOnce();
    expect(push).toHaveBeenCalledWith('/01-foundations/01-01-architecture-and-kraft/');
  });

  it('does not strip when href does not start with basePath segment', () => {
    const { event } = makeEvent({ href: '/other/path/' });
    const push = vi.fn();
    navigateToFrontierHref(event, { push }, '/kafka-cookbook');
    expect(push).toHaveBeenCalledWith('/other/path/');
  });

  it('avoids stripping a basePath that only matches as prefix substring', () => {
    // '/kafka' must not match '/kafka-cookbook/...': basePath stripping
    // requires the boundary slash so unrelated paths sharing a prefix stay
    // intact.
    const { event } = makeEvent({ href: '/kafka-cookbook/lesson/' });
    const push = vi.fn();
    navigateToFrontierHref(event, { push }, '/kafka');
    expect(push).toHaveBeenCalledWith('/kafka-cookbook/lesson/');
  });

  it('lets the browser handle modifier-clicks (no preventDefault, no push)', () => {
    for (const mod of ['metaKey', 'ctrlKey', 'shiftKey', 'altKey'] as const) {
      const { event, preventDefault } = makeEvent({ href: '/foo/', [mod]: true });
      const push = vi.fn();
      navigateToFrontierHref(event, { push }, '');
      expect(preventDefault).not.toHaveBeenCalled();
      expect(push).not.toHaveBeenCalled();
    }
  });

  it('ignores non-primary mouse buttons (middle-click)', () => {
    const { event, preventDefault } = makeEvent({ href: '/foo/', button: 1 });
    const push = vi.fn();
    navigateToFrontierHref(event, { push }, '');
    expect(preventDefault).not.toHaveBeenCalled();
    expect(push).not.toHaveBeenCalled();
  });

  it('skips when href is missing or "#"', () => {
    for (const href of [null, '', '#'] as const) {
      const { event, preventDefault } = makeEvent({ href });
      const push = vi.fn();
      navigateToFrontierHref(event, { push }, '');
      expect(preventDefault).not.toHaveBeenCalled();
      expect(push).not.toHaveBeenCalled();
    }
  });

  it('respects upstream preventDefault', () => {
    const { event, preventDefault } = makeEvent({ href: '/foo/', defaultPrevented: true });
    const push = vi.fn();
    navigateToFrontierHref(event, { push }, '');
    expect(preventDefault).not.toHaveBeenCalled();
    expect(push).not.toHaveBeenCalled();
  });
});
