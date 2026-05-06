export default function HomePage() {
  return (
    <main
      style={{
        maxWidth: 'var(--layout-content-max)',
        margin: '0 auto',
        padding: 'var(--space-12) var(--space-6)',
      }}
    >
      <h1
        style={{
          fontSize: 'var(--font-size-3xl)',
          fontWeight: 'var(--font-weight-bold)',
          lineHeight: 'var(--line-height-tight)',
          marginBottom: 'var(--space-4)',
        }}
      >
        Kafka Cookbook
      </h1>
      <p
        style={{
          fontSize: 'var(--font-size-lg)',
          color: 'var(--content-secondary)',
          lineHeight: 'var(--line-height-relaxed)',
        }}
      >
        Курс по Apache Kafka на Go. Полноценный фронтенд появится в следующих итерациях.
      </p>
    </main>
  );
}
