import styles from './LessonMeta.module.css';

type LessonMetaProps = {
  duration: string;
  tags?: string[];
};

export function LessonMeta({ duration, tags = [] }: LessonMetaProps) {
  return (
    <div className={styles.meta} aria-label="Метаданные урока">
      <span className={styles.duration}>{duration}</span>
      {tags.length > 0 && (
        <ul className={styles.tags}>
          {tags.map((tag) => (
            <li key={tag} className={styles.tag}>
              #{tag}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
