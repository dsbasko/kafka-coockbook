import { type Lang } from './lang';

/**
 * UI string dictionary. Every key here must exist in every entry of
 * UI_STRINGS — TypeScript enforces this via the `Record<Lang, UIDict>`
 * shape below. Add new keys here first, then translations.
 *
 * Scope: strings rendered by components listed in Task 10 of the i18n
 * plan (Header/Breadcrumbs, Sidebar, ProgramDrawer, LessonNav, Toc,
 * ReadingProgress, LessonLockedInterstitial, ThemeToggle, HomePage,
 * LessonPageLayout, not-found) plus a few keys for adjacent surfaces
 * already touched by lang switching (LanguageToggle aria, theme labels
 * previously lived in `lib/theme.ts`).
 */
export type UIDict = {
  // Sidebar
  sidebarLabel: string;
  navMainLabel: string;
  home: string;
  programCourse: string;
  githubRepo: string;

  // Header / Breadcrumbs
  breadcrumbsLabel: string;

  // ProgramDrawer
  close: string;
  moduleListLabel: string;
  lessonLockTitle: string;

  // LessonNav
  lessonNavLabel: string;
  prevLesson: string;
  nextLesson: string;

  // Toc
  tocLabel: string;

  // ReadingProgress
  readingProgressLabel: string;

  // LessonLockedInterstitial
  locked: string;
  moduleNumberPrefix: string;
  lockedTitle: string;
  lockedDesc: string;
  attemptedLessonLabel: string;
  attemptedYouTried: string;
  continueAction: string;
  startFromFirst: string;
  openProgram: string;
  courseProgress: string;
  progress: string;
  nextStep: string;
  untilThisLesson: string;

  // ThemeToggle (labels moved out of lib/theme.ts in Task 15)
  themeToggleLabel: string;
  themeLight: string;
  themeDark: string;
  themeSystem: string;

  // HomePage
  heroTitleLead: string;
  heroTitleAccent: string;
  heroTitleTail: string;
  continueLessonPrefix: string;
  startFromScratch: string;
  progressSummary: string;
  modulesLabel: string;
  lessonsLabel: string;
  durationLabel: string;
  stackLabel: string;
  moduleLockTitle: string;

  // LessonPageLayout
  lessonInfoLabel: string;

  // not-found
  notFoundTitle: string;
  notFoundDesc: string;
  goHome: string;

  // LanguageToggle (mounted in Task 11, aria-label drawn from here)
  language: string;

  // TranslationBanner (rendered on EN lesson pages that fall back to RU)
  translationFallbackTitle: string;
  translationFallbackBody: string;
};

export const UI_STRINGS: Record<Lang, UIDict> = {
  ru: {
    sidebarLabel: 'Боковая навигация',
    navMainLabel: 'Основная навигация',
    home: 'Главная',
    programCourse: 'Программа курса',
    githubRepo: 'Репозиторий на GitHub',

    breadcrumbsLabel: 'Хлебные крошки',

    close: 'Закрыть',
    moduleListLabel: 'Список модулей и уроков',
    lessonLockTitle: 'Урок откроется после прохождения предыдущих',

    lessonNavLabel: 'Навигация по урокам',
    prevLesson: '← Предыдущий урок',
    nextLesson: 'Следующий урок →',

    tocLabel: 'Содержание',

    readingProgressLabel: 'Прогресс чтения',

    locked: 'LOCKED',
    moduleNumberPrefix: 'Модуль',
    lockedTitle: 'Этот урок ещё впереди',
    lockedDesc:
      'Курс изучается по порядку — чтобы открыть этот шаг, сначала завершите предыдущие. Так контекст накапливается без пропусков.',
    attemptedLessonLabel: 'Урок, который вы открыли',
    attemptedYouTried: '/ вы пытались открыть',
    continueAction: 'Продолжить',
    startFromFirst: 'Начать с первого урока',
    openProgram: 'Открыть программу',
    courseProgress: 'Прогресс курса',
    progress: 'Прогресс',
    nextStep: 'Следующий шаг',
    untilThisLesson: 'До этого урока',

    themeToggleLabel: 'Тема оформления',
    themeLight: 'Светлая',
    themeDark: 'Тёмная',
    themeSystem: 'Системная',

    heroTitleLead: 'Kafka',
    heroTitleAccent: 'для тех, кто',
    heroTitleTail: 'пишет на Go',
    continueLessonPrefix: 'Продолжить · урок',
    startFromScratch: 'Начать с начала',
    progressSummary: 'Сводка прогресса',
    modulesLabel: 'Модулей',
    lessonsLabel: 'Уроков',
    durationLabel: 'Длительность',
    stackLabel: 'Стек',
    moduleLockTitle: 'Модуль откроется после прохождения предыдущих уроков',

    lessonInfoLabel: 'Сведения об уроке',

    notFoundTitle: 'Страница не найдена',
    notFoundDesc: 'Похоже, такой лекции в курсе нет. Вернитесь на главную.',
    goHome: 'На главную',

    language: 'Язык интерфейса',

    translationFallbackTitle: 'Перевод в процессе',
    translationFallbackBody:
      'Английская версия этой лекции пока не готова. Показан оригинал на русском.',
  },
  en: {
    sidebarLabel: 'Side navigation',
    navMainLabel: 'Main navigation',
    home: 'Home',
    programCourse: 'Course outline',
    githubRepo: 'GitHub repository',

    breadcrumbsLabel: 'Breadcrumbs',

    close: 'Close',
    moduleListLabel: 'Module and lesson list',
    lessonLockTitle: 'Lesson unlocks after the previous ones are complete',

    lessonNavLabel: 'Lesson navigation',
    prevLesson: '← Previous lesson',
    nextLesson: 'Next lesson →',

    tocLabel: 'Contents',

    readingProgressLabel: 'Reading progress',

    locked: 'LOCKED',
    moduleNumberPrefix: 'Module',
    lockedTitle: 'This lesson is still ahead',
    lockedDesc:
      'The course goes in order — to open this step, finish the previous ones first. Context builds up without gaps that way.',
    attemptedLessonLabel: 'Lesson you tried to open',
    attemptedYouTried: '/ you tried to open',
    continueAction: 'Continue',
    startFromFirst: 'Start from the first lesson',
    openProgram: 'Open outline',
    courseProgress: 'Course progress',
    progress: 'Progress',
    nextStep: 'Next step',
    untilThisLesson: 'Until this lesson',

    themeToggleLabel: 'Theme',
    themeLight: 'Light',
    themeDark: 'Dark',
    themeSystem: 'System',

    heroTitleLead: 'Kafka',
    heroTitleAccent: 'for people who',
    heroTitleTail: 'write Go',
    continueLessonPrefix: 'Continue · lesson',
    startFromScratch: 'Start over',
    progressSummary: 'Progress summary',
    modulesLabel: 'Modules',
    lessonsLabel: 'Lessons',
    durationLabel: 'Duration',
    stackLabel: 'Stack',
    moduleLockTitle: 'Module unlocks after the previous lessons are complete',

    lessonInfoLabel: 'Lesson info',

    notFoundTitle: 'Page not found',
    notFoundDesc: 'There is no such lesson in the course. Head back home.',
    goHome: 'Go home',

    language: 'Interface language',

    translationFallbackTitle: 'Translation in progress',
    translationFallbackBody:
      'The English version of this lesson is not ready yet. Showing the original Russian text.',
  },
};

/**
 * Server-friendly accessor. Server components receive `lang` via route
 * params; pass it in here. Client components should prefer `useT()` from
 * `web/lib/use-i18n.ts` instead — it reads `lang` from `useParams()`.
 */
export function getDict(lang: Lang): UIDict {
  return UI_STRINGS[lang];
}
