import styles from './page.module.css';

export default function HomePage() {
  return (
    <div className={styles.wrapper}>
      <h1 className={styles.title}>Главная курса</h1>
      <p className={styles.lead}>
        Заглушка-каркас. Список модулей и описание курса появятся в следующих итерациях.
      </p>
    </div>
  );
}
