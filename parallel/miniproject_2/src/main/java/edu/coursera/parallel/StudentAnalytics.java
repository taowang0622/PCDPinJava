package edu.coursera.parallel;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A simple wrapper class for various analytics methods.
 */
public final class StudentAnalytics {
    /**
     * Sequentially computes the average age of all actively enrolled students
     * using loops.
     *
     * @param studentArray Student data for the class.
     * @return Average age of enrolled students
     */
    public double averageAgeOfEnrolledStudentsImperative(
            final Student[] studentArray) {
        List<Student> activeStudents = new ArrayList<Student>();

        for (Student s : studentArray) {
            if (s.checkIsCurrent()) {
                activeStudents.add(s);
            }
        }

        double ageSum = 0.0;
        for (Student s : activeStudents) {
            ageSum += s.getAge();
        }

        return ageSum / (double) activeStudents.size();
    }

    /**
     * TODO compute the average age of all actively enrolled students using
     * parallel streams. This should mirror the functionality of
     * averageAgeOfEnrolledStudentsImperative. This method should not use any
     * loops.
     *
     * @param studentArray Student data for the class.
     * @return Average age of enrolled students
     */
    public double averageAgeOfEnrolledStudentsParallelStream(
            final Student[] studentArray) {
        return Stream.of(studentArray).parallel().filter(student -> student.checkIsCurrent())
                .mapToDouble(student -> student.getAge()).average().getAsDouble();

    }

    /**
     * Sequentially computes the most common first name out of all students that
     * are no longer active in the class using loops.
     *
     * @param studentArray Student data for the class.
     * @return Most common first name of inactive students
     */
    public String mostCommonFirstNameOfInactiveStudentsImperative(
            final Student[] studentArray) {
        List<Student> inactiveStudents = new ArrayList<Student>();

        for (Student s : studentArray) {
            if (!s.checkIsCurrent()) {
                inactiveStudents.add(s);
            }
        }

        Map<String, Integer> nameCounts = new HashMap<String, Integer>();

        for (Student s : inactiveStudents) {
            if (nameCounts.containsKey(s.getFirstName())) {
                nameCounts.put(s.getFirstName(),
                        new Integer(nameCounts.get(s.getFirstName()) + 1));
            } else {
                nameCounts.put(s.getFirstName(), 1);
            }
        }

        String mostCommon = null;
        int mostCommonCount = -1;
        for (Map.Entry<String, Integer> entry : nameCounts.entrySet()) {
            if (mostCommon == null || entry.getValue() > mostCommonCount) {
                mostCommon = entry.getKey();
                mostCommonCount = entry.getValue();
            }
        }

        return mostCommon;
    }

    /**
     * TODO compute the most common first name out of all students that are no
     * longer active in the class using parallel streams. This should mirror the
     * functionality of mostCommonFirstNameOfInactiveStudentsImperative. This
     * method should not use any loops.
     *
     * @param studentArray Student data for the class.
     * @return Most common first name of inactive students
     */
    public String mostCommonFirstNameOfInactiveStudentsParallelStream(
            final Student[] studentArray) {
//        return Stream.of(studentArray).parallel().filter(student -> !student.checkIsCurrent())
//                .collect(Collectors.groupingBy(Student::getFirstName, Collectors.counting()))
//                .entrySet().parallelStream().max(Map.Entry.comparingByValue())
//                .get().getKey();

        // no need to create the second parallel stream
        // the first one is OK as the data set is so huge that parallelism is needed
        // after the first one's terminal operation ".collect", the data set shrinks a lot
        // after that, creating a parallel stream is expensive!!
        // REMEMBER: STREAM IS EXPENSIVE!!!!!!! ==> AFTER CHANGING THE SECOND PARALLEL STREAM TO STREAM, speedup goes up from 0.2 to 1.8!!!!
//        return Stream.of(studentArray).parallel().filter(student -> !student.checkIsCurrent())
//                .collect(Collectors.groupingBy(Student::getFirstName, Collectors.counting()))
//                .entrySet().stream().max(Map.Entry.comparingByValue())
//                .get().getKey();


        //  A for loop through an array is extremely lightweight both in terms of heap and CPU usage
        // ONLY WHEN DATA SET IS HUGE, STREAM CAN BE USED WITH PARALLELISM!!!
        // OTHERWISE, A SIMPLE FOR LOOP IS MUCH MUCH MUCH FASTER!!!
        Set<Map.Entry<String, Long>> entries = Stream.of(studentArray).parallel().filter(student -> !student.checkIsCurrent())
                .collect(Collectors.groupingBy(Student::getFirstName, Collectors.counting()))
                .entrySet();
        long maxCount = 0;
        String mostCommonFirstName = "";
        for (Map.Entry<String, Long> entry : entries) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                mostCommonFirstName = entry.getKey();
            }
        }
        return mostCommonFirstName;
    }

    /**
     * Sequentially computes the number of students who have failed the course
     * who are also older than 20 years old. A failing grade is anything below a
     * 65. A student has only failed the course if they have a failing grade and
     * they are not currently active.
     *
     * @param studentArray Student data for the class.
     * @return Number of failed grades from students older than 20 years old.
     */
    public int countNumberOfFailedStudentsOlderThan20Imperative(
            final Student[] studentArray) {
        int count = 0;
        for (Student s : studentArray) {
            if (!s.checkIsCurrent() && s.getAge() > 20 && s.getGrade() < 65) {
                count++;
            }
        }
        return count;
    }

    /**
     * TODO compute the number of students who have failed the course who are
     * also older than 20 years old. A failing grade is anything below a 65. A
     * student has only failed the course if they have a failing grade and they
     * are not currently active. This should mirror the functionality of
     * countNumberOfFailedStudentsOlderThan20Imperative. This method should not
     * use any loops.
     *
     * @param studentArray Student data for the class.
     * @return Number of failed grades from students older than 20 years old.
     */
    public int countNumberOfFailedStudentsOlderThan20ParallelStream(
            final Student[] studentArray) {
        return (int) Arrays.stream(studentArray)
                .parallel()
                .filter(student -> !student.checkIsCurrent() && student.getAge() > 20 && student.getGrade() < 65).count();
    }
}
