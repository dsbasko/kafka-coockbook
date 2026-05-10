// Russian pluralization plus duration parsing/formatting for course metadata.
// `course.yaml` stores durations as strings like "45m" or "1h 30m"; we keep
// them as authored and convert to minutes only when math is needed.

export type PluralForms = readonly [one: string, few: string, many: string];

export function pluralize(n: number, forms: PluralForms): string {
  const abs = Math.abs(n) % 100;
  const lastDigit = abs % 10;
  if (abs > 10 && abs < 20) return forms[2];
  if (lastDigit > 1 && lastDigit < 5) return forms[1];
  if (lastDigit === 1) return forms[0];
  return forms[2];
}

export const LESSON_FORMS: PluralForms = ['урок', 'урока', 'уроков'];
export const MODULE_FORMS: PluralForms = ['модуль', 'модуля', 'модулей'];

export function parseDurationMin(input: string): number {
  let total = 0;
  const h = input.match(/(\d+)\s*h/);
  const m = input.match(/(\d+)\s*m/);
  if (h) total += parseInt(h[1], 10) * 60;
  if (m) total += parseInt(m[1], 10);
  return total;
}

export function formatDurationHm(min: number): string {
  if (min <= 0) return '0 мин';
  const h = Math.floor(min / 60);
  const m = min % 60;
  if (h === 0) return `${m} мин`;
  if (m === 0) return `${h} ч`;
  return `${h} ч ${m} мин`;
}
