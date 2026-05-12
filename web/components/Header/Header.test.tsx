import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { Header } from './Header';
import { UI_STRINGS } from '@/lib/i18n';

describe('Header', () => {
  it('uses the RU breadcrumbs aria-label when lang=ru', () => {
    const html = renderToStaticMarkup(<Header lang="ru" />);
    expect(html).toContain(`aria-label="${UI_STRINGS.ru.breadcrumbsLabel}"`);
  });

  it('uses the EN breadcrumbs aria-label when lang=en', () => {
    const html = renderToStaticMarkup(<Header lang="en" />);
    expect(html).toContain(`aria-label="${UI_STRINGS.en.breadcrumbsLabel}"`);
  });

  it('falls back to the "Kafka Cookbook" brand when no breadcrumbs slot is given', () => {
    const html = renderToStaticMarkup(<Header lang="en" />);
    expect(html).toContain('Kafka Cookbook');
  });

  it('renders the provided breadcrumbs and actions slots', () => {
    const html = renderToStaticMarkup(
      <Header
        lang="en"
        breadcrumbs={<span data-testid="crumbs">crumbs-slot</span>}
        actions={<span data-testid="actions">actions-slot</span>}
      />,
    );
    expect(html).toContain('crumbs-slot');
    expect(html).toContain('actions-slot');
  });
});
