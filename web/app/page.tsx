import Link from 'next/link';
import { loadCourse } from '@/lib/course';
import styles from './page.module.css';

export default function HomePage() {
  const course = loadCourse();
  return (
    <div className={styles.wrapper}>
      <h1 className={styles.title}>{course.title}</h1>
      <p className={styles.lead}>{collapseWhitespace(course.description)}</p>
      <ol className={styles.modules}>
        {course.modules.map((mod, index) => (
          <li key={mod.id} className={styles.module}>
            <Link href={`/${mod.id}`} className={styles.moduleLink}>
              <span className={styles.moduleIndex}>
                {String(index + 1).padStart(2, '0')}
              </span>
              <span className={styles.moduleBody}>
                <span className={styles.moduleTitle}>{mod.title}</span>
                <span className={styles.moduleDescription}>
                  {collapseWhitespace(mod.description)}
                </span>
                <span className={styles.moduleStats}>
                  {mod.lessons.length} {pluralizeLesson(mod.lessons.length)}
                </span>
              </span>
            </Link>
          </li>
        ))}
      </ol>
    </div>
  );
}

function collapseWhitespace(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}

function pluralizeLesson(count: number): string {
  const mod10 = count % 10;
  const mod100 = count % 100;
  if (mod10 === 1 && mod100 !== 11) return 'урок';
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return 'урока';
  return 'уроков';
}
