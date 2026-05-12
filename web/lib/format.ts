import type { Lang } from './lang';

export type PluralForms = readonly [one: string, few: string, many: string];

export function pluralize(n: number, forms: PluralForms): string {
  const abs = Math.abs(n) % 100;
  const lastDigit = abs % 10;
  if (abs > 10 && abs < 20) return forms[2];
  if (lastDigit > 1 && lastDigit < 5) return forms[1];
  if (lastDigit === 1) return forms[0];
  return forms[2];
}

const LESSON_FORMS_RU: PluralForms = ['урок', 'урока', 'уроков'];
const MODULE_FORMS_RU: PluralForms = ['модуль', 'модуля', 'модулей'];

export function formatLessonCount(n: number, lang: Lang): string {
  if (lang === 'en') return `${n} ${n === 1 ? 'lesson' : 'lessons'}`;
  return `${n} ${pluralize(n, LESSON_FORMS_RU)}`;
}

export function formatModuleCount(n: number, lang: Lang): string {
  if (lang === 'en') return `${n} ${n === 1 ? 'module' : 'modules'}`;
  return `${n} ${pluralize(n, MODULE_FORMS_RU)}`;
}

export function parseDurationMin(input: string): number {
  let total = 0;
  const h = input.match(/(\d+)\s*h/);
  const m = input.match(/(\d+)\s*m/);
  if (h) total += parseInt(h[1], 10) * 60;
  if (m) total += parseInt(m[1], 10);
  return total;
}

export function formatDurationHm(min: number, lang: Lang): string {
  if (lang === 'en') {
    if (min <= 0) return '0 min';
    const h = Math.floor(min / 60);
    const m = min % 60;
    if (h === 0) return `${m} min`;
    if (m === 0) return `${h} h`;
    return `${h} h ${m} min`;
  }
  if (min <= 0) return '0 мин';
  const h = Math.floor(min / 60);
  const m = min % 60;
  if (h === 0) return `${m} мин`;
  if (m === 0) return `${h} ч`;
  return `${h} ч ${m} мин`;
}

export function formatDurationShort(min: number, lang: Lang): string {
  return lang === 'en' ? `${min}m` : `${min}м`;
}
