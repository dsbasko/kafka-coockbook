import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { renderLessonMarkdown } from './markdown';

const baseOpts = {
  moduleId: '02-producer',
  slug: '02-03-idempotent-producer',
  basePath: '/kafka-cookbook',
};

async function renderHtml(source: string): Promise<string> {
  const { content } = await renderLessonMarkdown(source, baseOpts);
  return renderToStaticMarkup(content);
}

describe('renderLessonMarkdown', () => {
  it('renders headings with anchor links via rehype-slug + autolink-headings', async () => {
    const html = await renderHtml('# Главное\n\n## Подробности\n\nтекст.');
    expect(html).toContain('<h1');
    expect(html).toContain('<h2');
    expect(html).toContain('id="подробности"');
    expect(html).toContain('class="heading-anchor"');
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

  it('renders blockquotes (callouts deferred to Task 12)', async () => {
    const html = await renderHtml('> Просто блок цитаты.');
    expect(html).toContain('<blockquote');
    expect(html).toContain('Просто блок цитаты.');
  });
});
