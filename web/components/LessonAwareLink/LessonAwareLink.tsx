'use client';

import type { AnchorHTMLAttributes, ReactNode } from 'react';
import { LockIcon } from '@/components/ProgramDrawer/LockIcon';
import { stripLangFromPath } from '@/lib/lang';
import styles from './LessonAwareLink.module.css';

type LessonAwareLinkProps = AnchorHTMLAttributes<HTMLAnchorElement> & {
  basePath?: string;
  children?: ReactNode;
};

/**
 * MDX `<a>` replacement that participates in the gate. The rendered shape is
 * always a plain anchor — the inline gate-mark script flips `data-locked` on
 * any link whose `data-lesson-key` points at a still-locked lesson, and CSS
 * dims the row + reveals the inline lock badge from there. Click handler
 * uses the attribute (not React state) so SSR markup and post-hydration
 * markup are identical and there's no flash.
 */
export function LessonAwareLink({
  href,
  children,
  basePath = '',
  ...rest
}: LessonAwareLinkProps) {
  const target = href ? matchLessonHref(href, basePath) : null;
  const dataKey = target ? `${target.moduleId}/${target.slug}` : undefined;

  return (
    <a
      href={href}
      data-lesson-key={dataKey}
      onClick={
        dataKey
          ? (e) => {
              if (e.currentTarget.getAttribute('data-locked') === 'true') {
                e.preventDefault();
              }
            }
          : undefined
      }
      {...rest}
      // Why: the inline gate-mark script (runs before hydration) stamps
      // `data-locked` / `aria-disabled` / `tabindex` on any anchor whose
      // `data-lesson-key` points at a still-locked lesson. React's
      // hydration would otherwise flag those as "extra attributes from
      // the server" because the virtual DOM doesn't include them.
      suppressHydrationWarning
    >
      {children}
      {dataKey && (
        <span
          className={styles.lockBadge}
          data-lock-badge=""
          aria-hidden="true"
        >
          <LockIcon />
        </span>
      )}
    </a>
  );
}

function matchLessonHref(
  href: string,
  basePath: string,
): { moduleId: string; slug: string } | null {
  if (href.length === 0) return null;
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(href)) return null;
  if (/^(?:mailto:|tel:|data:)/i.test(href)) return null;
  if (href.startsWith('#')) return null;

  let path = href;
  if (basePath && path.startsWith(`${basePath}/`)) {
    path = path.slice(basePath.length);
  }
  if (!path.startsWith('/')) return null;

  const withoutHash = path.split('#')[0]?.split('?')[0] ?? '';
  const { rest } = stripLangFromPath(withoutHash);
  const segments = rest.split('/').filter(Boolean);
  if (segments.length < 2) return null;

  return { moduleId: segments[0], slug: segments[1] };
}
