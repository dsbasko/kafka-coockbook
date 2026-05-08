import type { ReactNode } from 'react';
import { CodeBlock } from '@/components/CodeBlock';
import { Callout, isCalloutType } from '@/components/Callout';

type FigureProps = {
  children?: ReactNode;
} & Record<string, unknown>;

export function MarkdownFigure(props: FigureProps) {
  if (!('data-rehype-pretty-code-figure' in props)) {
    const { children, ...rest } = props;
    return <figure {...(rest as Record<string, string>)}>{children}</figure>;
  }
  const language = readLanguage(props);
  return <CodeBlock language={language}>{props.children}</CodeBlock>;
}

type AsideProps = {
  children?: ReactNode;
} & Record<string, unknown>;

export function MarkdownAside(props: AsideProps) {
  const calloutType = props['data-callout-type'];
  if (isCalloutType(calloutType)) {
    return <Callout type={calloutType}>{props.children}</Callout>;
  }
  const { children, ...rest } = props;
  return <aside {...(rest as Record<string, string>)}>{children}</aside>;
}

function readLanguage(props: Record<string, unknown>): string {
  const v = props['data-language'];
  return typeof v === 'string' && v.length > 0 ? v : 'plaintext';
}
