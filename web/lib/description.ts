const MAX_DESCRIPTION_LENGTH = 200;

export function extractDescription(markdown: string): string | null {
  const lines = markdown.split('\n');
  let inFence = false;
  const buffer: string[] = [];

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (line.startsWith('```')) {
      if (inFence && buffer.length > 0) break;
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    if (line.length === 0) {
      if (buffer.length > 0) break;
      continue;
    }
    if (line.startsWith('#')) continue;
    if (line.startsWith('>') || line.startsWith('|') || /^[-*+]\s/.test(line)) continue;
    buffer.push(line);
  }

  if (buffer.length === 0) return null;

  let text = buffer.join(' ');
  text = text
    .replace(/`([^`]+)`/g, '$1')
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/\*([^*]+)\*/g, '$1')
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/\s+/g, ' ')
    .trim();

  if (text.length > MAX_DESCRIPTION_LENGTH) {
    const cut = text.slice(0, MAX_DESCRIPTION_LENGTH);
    const lastSpace = cut.lastIndexOf(' ');
    text = (lastSpace > 0 ? cut.slice(0, lastSpace) : cut).trim() + '…';
  }

  return text;
}
