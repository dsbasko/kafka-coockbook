import { loadCourse } from '@/lib/course-loader';
import { HomePage } from '@/components/HomePage';

export default function Page() {
  const course = loadCourse();
  // The course YAML doesn't carry a level field today; surface a sensible
  // default until it does, so the stats card has all four cells filled.
  return <HomePage course={course} level="Go" />;
}
