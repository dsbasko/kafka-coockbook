'use client';

import { useState, type ReactNode } from 'react';
import { usePathname } from 'next/navigation';
import { Sidebar } from '@/components/Sidebar';
import { Header } from '@/components/Header';
import { Breadcrumbs } from '@/components/Header/Breadcrumbs';
import { HeaderLessonNav } from '@/components/Header/HeaderLessonNav';
import { ProgramDrawer } from '@/components/ProgramDrawer';
import { ProgressBar } from '@/components/ProgressBar';
import { type Course, getNextLesson, getPrevLesson } from '@/lib/course';
import styles from './AppShell.module.css';

type AppShellProps = {
  children: ReactNode;
  course: Course;
};

export function AppShell({ children, course }: AppShellProps) {
  const pathname = usePathname() ?? '/';
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);

  const segments = pathname.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
  const moduleId = segments[0];
  const lessonSlug = segments[1];

  const currentModule = moduleId
    ? course.modules.find((m) => m.id === moduleId)
    : undefined;
  const currentLesson =
    currentModule && lessonSlug
      ? currentModule.lessons.find((l) => l.slug === lessonSlug)
      : undefined;

  const prev =
    currentModule && currentLesson
      ? getPrevLesson(course, currentModule.id, currentLesson.slug)
      : null;
  const next =
    currentModule && currentLesson
      ? getNextLesson(course, currentModule.id, currentLesson.slug)
      : null;

  const breadcrumbs = (
    <Breadcrumbs
      moduleId={currentModule?.id}
      moduleTitle={currentModule?.title}
      lessonTitle={currentLesson?.title}
    />
  );

  const actions = (
    <>
      <ProgressBar />
      {currentLesson ? <HeaderLessonNav prev={prev} next={next} /> : null}
    </>
  );

  return (
    <div className={styles.shell}>
      <Sidebar onProgramClick={() => setIsDrawerOpen(true)} />
      <ProgramDrawer
        course={course}
        currentModuleId={currentModule?.id}
        currentSlug={currentLesson?.slug}
        isOpen={isDrawerOpen}
        onClose={() => setIsDrawerOpen(false)}
      />
      <div className={styles.body}>
        <Header breadcrumbs={breadcrumbs} actions={actions} />
        <main className={styles.main}>{children}</main>
      </div>
    </div>
  );
}
