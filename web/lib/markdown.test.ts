import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { renderLessonMarkdown } from './markdown';
import type { Course } from './course';

const COURSE: Course = {
  title: 'Kafka Cookbook',
  description: 'desc',
  basePath: '/kafka-cookbook',
  repoUrl: 'https://github.com/dsbasko/kafka-cookbook',
  modules: [
    {
      id: '02-producer',
      title: 'Producer',
      description: 'desc',
      lessons: [
        { slug: '02-03-idempotent-producer', title: 'Idempotent', duration: '30m', tags: [] },
        { slug: '02-04-batching-and-throughput', title: 'Batching', duration: '30m', tags: [] },
      ],
    },
    {
      id: '03-consumer',
      title: 'Consumer',
      description: 'desc',
      lessons: [
        { slug: '03-01-groups-and-rebalance', title: 'Groups', duration: '30m', tags: [] },
      ],
    },
  ],
};

const baseOpts = {
  moduleId: '02-producer',
  slug: '02-03-idempotent-producer',
  basePath: '/kafka-cookbook',
  course: COURSE,
};

async function renderHtml(source: string): Promise<string> {
  const { content } = await renderLessonMarkdown(source, baseOpts);
  return renderToStaticMarkup(content);
}

describe('renderLessonMarkdown', () => {
  it('renders headings with anchor links via rehype-slug + autolink-headings', async () => {
    const html = await renderHtml('# Главное\n\n## Подробности\n\nтекст.');
    // Leading H1 is stripped: the page H1 comes from course.yaml via LessonLayout,
    // so the README's own H1 is removed to avoid duplicate top-level headings.
    expect(html).not.toContain('<h1');
    expect(html).toContain('<h2');
    expect(html).toContain('id="подробности"');
    expect(html).toContain('class="heading-anchor"');
  });

  it('strips only the leading H1, leaving deeper headings and inline content intact', async () => {
    const html = await renderHtml(
      '# Заголовок урока\n\n## Раздел\n\nабзац.\n\n# Поздний H1 в теле',
    );
    // The leading H1 is dropped, but a non-leading H1 inside the body stays —
    // we don't blanket-remove every H1, only the first content node.
    expect(html).toContain('<h1');
    expect(html).toContain('Поздний H1 в теле');
    expect(html).not.toContain('Заголовок урока');
  });

  it('renders GFM tables', async () => {
    const html = await renderHtml(
      '| тип | значение |\n| --- | --- |\n| acks | all |\n',
    );
    expect(html).toContain('<table');
    expect(html).toContain('<th');
    expect(html).toContain('acks');
  });

  it('does not choke on ASCII arrows like <-- or generic placeholders <T>', async () => {
    // These tokens would crash an MDX parser but must work in plain markdown.
    const source = [
      '## Поток',
      '',
      'producer <-- broker --> consumer',
      '',
      'Используйте `<broker-id>` и generic `<T>` в типах.',
    ].join('\n');
    const html = await renderHtml(source);
    expect(html).toContain('producer');
    expect(html).toContain('broker');
    expect(html).toContain('consumer');
  });

  it('highlights fenced Go code via rehype-pretty-code with dual themes', async () => {
    const source = ['```go', 'package main', '', 'func main() {}', '```'].join('\n');
    const html = await renderHtml(source);
    expect(html).toContain('data-rehype-pretty-code-figure');
    expect(html.toLowerCase()).toContain('data-language="go"');
    // dual-theme: rehype-pretty-code 0.13 emits one <pre> with both theme names
    // and stamps --shiki-light / --shiki-dark CSS vars on each token <span>.
    expect(html).toContain('data-theme="github-light night-owl"');
    expect(html).toContain('--shiki-light');
    expect(html).toContain('--shiki-dark');
  });

  it('wraps code blocks with CodeBlock header (language label + copy button)', async () => {
    const source = ['```ts', 'const x: number = 1;', '```'].join('\n');
    const html = await renderHtml(source);
    expect(html).toContain('Скопировать');
    // language label appears in the header
    expect(html.toLowerCase()).toContain('>ts<');
    // wrapper figure preserves rehype-pretty-code marker for CSS hooks
    expect(html).toContain('data-rehype-pretty-code-figure');
    // inner <pre> still rendered with dual-theme attribute
    expect(html).toContain('<pre');
    expect(html).toContain('data-theme="github-light night-owl"');
  });

  it('falls back to "plaintext" language label when fence has no info string', async () => {
    const source = ['```', 'just text', '```'].join('\n');
    const html = await renderHtml(source);
    expect(html).toContain('Скопировать');
    expect(html.toLowerCase()).toContain('plaintext');
  });

  it('rewrites lesson images to /static/lectures/<module>/<slug>/images/<file>', async () => {
    const html = await renderHtml('![alt](./images/diagram.png)');
    expect(html).toContain(
      '/kafka-cookbook/static/lectures/02-producer/02-03-idempotent-producer/images/diagram.png',
    );
  });

  it('throws on disallowed image paths (interim Task 6 contract)', async () => {
    await expect(renderHtml('![alt](../shared/foo.png)')).rejects.toThrow(
      /remark-lesson-images/,
    );
  });

  it('renders inline code without breaking the pipeline', async () => {
    const html = await renderHtml('Use `KAFKA_BOOTSTRAP=...`.');
    expect(html).toContain('<code');
    expect(html).toContain('KAFKA_BOOTSTRAP');
  });

  it('renders plain blockquotes without alert markup', async () => {
    const html = await renderHtml('> Просто блок цитаты.');
    expect(html).toContain('<blockquote');
    expect(html).toContain('Просто блок цитаты.');
    expect(html).not.toContain('data-callout-type');
  });

  it('renders GitHub-alert blockquotes as Callout asides with Russian titles', async () => {
    const html = await renderHtml('> [!NOTE]\n> Это заметка.');
    expect(html).toContain('data-callout-type="note"');
    expect(html).toContain('Заметка');
    expect(html).toContain('Это заметка.');
    // upstream auto-title (uppercase NOTE) must be stripped by rehype-callout
    expect(html).not.toContain('>NOTE<');
    expect(html).not.toContain('markdown-alert-title');
  });

  it('supports every callout type with the correct Russian label', async () => {
    const cases: Array<[string, string, string]> = [
      ['NOTE', 'note', 'Заметка'],
      ['TIP', 'tip', 'Подсказка'],
      ['WARNING', 'warning', 'Внимание'],
      ['IMPORTANT', 'important', 'Важно'],
      ['CAUTION', 'caution', 'Осторожно'],
    ];
    for (const [marker, type, label] of cases) {
      const html = await renderHtml(`> [!${marker}]\n> текст`);
      expect(html).toContain(`data-callout-type="${type}"`);
      expect(html).toContain(label);
    }
  });
});
