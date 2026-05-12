import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { LessonPageLayout } from './LessonPageLayout';
import { UI_STRINGS } from '@/lib/i18n';

describe('LessonPageLayout', () => {
  it('renders the side aside with the RU info label when lang=ru and a side slot is present', () => {
    const html = renderToStaticMarkup(
      <LessonPageLayout
        lang="ru"
        title="Title"
        tocSlot={<span>toc</span>}
      >
        body
      </LessonPageLayout>,
    );
    expect(html).toContain(`aria-label="${UI_STRINGS.ru.lessonInfoLabel}"`);
  });

  it('renders the side aside with the EN info label when lang=en', () => {
    const html = renderToStaticMarkup(
      <LessonPageLayout
        lang="en"
        title="Title"
        tocSlot={<span>toc</span>}
      >
        body
      </LessonPageLayout>,
    );
    expect(html).toContain(`aria-label="${UI_STRINGS.en.lessonInfoLabel}"`);
  });

  it('omits the side aside entirely when neither tocSlot nor sideMetaSlot is provided', () => {
    const html = renderToStaticMarkup(
      <LessonPageLayout lang="en" title="Title">
        body
      </LessonPageLayout>,
    );
    expect(html).not.toContain('aria-label');
  });

  it('renders the title and children passed in', () => {
    const html = renderToStaticMarkup(
      <LessonPageLayout lang="en" title="My lesson">
        <p>lesson-body</p>
      </LessonPageLayout>,
    );
    expect(html).toContain('My lesson');
    expect(html).toContain('lesson-body');
  });
});
