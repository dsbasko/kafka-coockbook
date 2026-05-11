import type { MouseEvent } from 'react';

type AppRouter = {
  push: (href: string) => void;
};

/**
 * Click handler for "Continue" CTAs that the gate-paint script rewrites in the
 * DOM. The Next.js `<Link>` keeps its prop `href` (set to a static fallback) so
 * SSR is stable, but at click time we must navigate to whatever the inline
 * gate-paint script wrote into the anchor's `href` attribute — otherwise the
 * label says "lesson N" while the prop-driven router.push lands on the
 * fallback. Behaves like a normal anchor for modifier / non-primary clicks so
 * cmd-click etc. still open the painted href in a new tab.
 */
export function navigateToFrontierHref(
  event: MouseEvent<HTMLAnchorElement>,
  router: AppRouter,
  basePath: string,
): void {
  if (
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  ) {
    return;
  }
  const href = event.currentTarget.getAttribute('href');
  if (!href || href === '#') return;
  event.preventDefault();
  const stripped =
    basePath && href.startsWith(`${basePath}/`) ? href.slice(basePath.length) : href;
  router.push(stripped);
}
