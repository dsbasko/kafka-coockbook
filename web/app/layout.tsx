import type { Metadata } from 'next';
import { Manrope, JetBrains_Mono } from 'next/font/google';
import '@/styles/globals.css';

const manrope = Manrope({
  subsets: ['latin', 'cyrillic'],
  display: 'swap',
  variable: '--font-ui',
});

const jetbrains = JetBrains_Mono({
  subsets: ['latin', 'cyrillic'],
  display: 'swap',
  variable: '--font-mono',
});

export const metadata: Metadata = {
  title: 'Kafka Cookbook',
  description:
    'Курс по Apache Kafka на Go: продюсеры, консьюмеры, надёжность, контракты, стримы, эксплуатация и use cases.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ru" data-theme="light" className={`${manrope.variable} ${jetbrains.variable}`}>
      <body>{children}</body>
    </html>
  );
}
