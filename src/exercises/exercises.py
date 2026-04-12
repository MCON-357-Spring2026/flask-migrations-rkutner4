"""Exercises: ORM fundamentals.

Implement the TODO functions. Autograder will test them.
"""

from __future__ import annotations

from typing import Optional
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from src.exercises.extensions import db
from src.exercises.models import Student, Grade, Assignment


# ===== BASIC CRUD =====

def create_student(name: str, email: str) -> Student:
    """TODO: Create and commit a Student; handle duplicate email.

    If email is duplicate:
      - rollback
      - raise ValueError("duplicate email")
    """
    student = Student(name=name, email=email)
    db.session.add(student)

    try:
        db.session.commit()
        return student
    except IntegrityError:
        db.session.rollback()
        raise ValueError("duplicate email")

def find_student_by_email(email: str) -> Optional[Student]:
    """TODO: Return Student by email or None."""
    return Student.query.filter_by(email=email).first()

def add_grade(student_id: int, assignment_id: int, score: int) -> Grade:
    """TODO: Add a Grade for the student+assignment and commit.

    If student doesn't exist: raise LookupError
    If assignment doesn't exist: raise LookupError
    If duplicate grade: raise ValueError("duplicate grade")
    """
    student = Student.query.get(student_id)
    if not student:
        raise LookupError("student not found")

    assignment = Assignment.query.get(assignment_id)
    if not assignment:
        raise LookupError("assignment not found")

    grade = Grade(
        student_id=student_id,
        assignment_id=assignment_id,
        score=score
    )

    db.session.add(grade)

    try:
        db.session.commit()
        return grade
    except IntegrityError:
        db.session.rollback()
        raise ValueError("duplicate grade")


def average_percent(student_id: int) -> float:
    """TODO: Return student's average percent across assignments.

    percent per grade = score / assignment.max_points * 100

    If student doesn't exist: raise LookupError
    If student has no grades: return 0.0
    """
    student = Student.query.get(student_id)
    if not student:
        raise LookupError("student not found")

    grades = Grade.query.filter_by(student_id=student_id).all()

    if not grades:
        return 0.0

    total = 0.0

    for g in grades:
        total += (g.score / g.assignment.max_points) * 100

    return total / len(grades)


# ===== QUERYING & FILTERING =====

def get_all_students() -> list[Student]:
    """TODO: Return all students in database, ordered by name."""
    return Student.query.order_by(Student.name.asc()).all()

def get_assignment_by_title(title: str) -> Optional[Assignment]:
    """TODO: Return assignment by title or None."""
    return Assignment.query.filter_by(title=title).first()

def get_student_grades(student_id: int) -> list[Grade]:
    """TODO: Return all grades for a student, ordered by assignment title.

    If student doesn't exist: raise LookupError
    """
    student = Student.query.get(student_id)
    if not student:
        raise LookupError("student not found")

    return (
        Grade.query
        .join(Assignment)
        .filter(Grade.student_id == student_id)
        .order_by(Assignment.title.asc())
        .all()
    )

def get_grades_for_assignment(assignment_id: int) -> list[Grade]:
    """TODO: Return all grades for an assignment, ordered by student name.

    If assignment doesn't exist: raise LookupError
    """
    assignment = Assignment.query.get(assignment_id)
    if not assignment:
        raise LookupError("assignment not found")

    return (
        Grade.query
        .join(Student)
        .filter(Grade.assignment_id == assignment_id)
        .order_by(Student.name.asc())
        .all()
    )

# ===== AGGREGATION =====

def total_student_grade_count() -> int:
    """TODO: Return total number of grades in database."""
    return Grade.query.count()

def highest_score_on_assignment(assignment_id: int) -> Optional[int]:
    """TODO: Return the highest score on an assignment, or None if no grades.

    If assignment doesn't exist: raise LookupError
    """
    assignment = Assignment.query.get(assignment_id)
    if not assignment:
        raise LookupError("assignment not found")

    highest = (
        db.session.query(func.max(Grade.score))
        .filter(Grade.assignment_id == assignment_id)
        .scalar()
    )

    return highest

def class_average_percent() -> float:
    """Return average percent across all students and all assignments."""

    results = (
        db.session.query(
            (Grade.score / Assignment.max_points) * 100
        )
        .join(Assignment, Grade.assignment_id == Assignment.id)
        .all()
    )

    if not results:
        return 0.0

    values = [float(r[0]) for r in results]
    return float(sum(values) / len(values))

def student_grade_count(student_id: int) -> int:
    """TODO: Return number of grades for a student.

    If student doesn't exist: raise LookupError
    """
    student = Student.query.get(student_id)
    if not student:
        raise LookupError("student not found")

    return Grade.query.filter_by(student_id=student_id).count()


# ===== UPDATING & DELETION =====

def update_student_email(student_id: int, new_email: str) -> Student:
    """TODO: Update a student's email and commit.

    If student doesn't exist: raise LookupError
    If new email is duplicate: rollback and raise ValueError("duplicate email")
    Return the updated student.
    """
    student = Student.query.get(student_id)
    if not student:
        raise LookupError("student not found")

    student.email = new_email

    try:
        db.session.commit()
        return student
    except IntegrityError:
        db.session.rollback()
        raise ValueError("duplicate email")

def delete_student(student_id: int) -> None:
    """TODO: Delete a student and all their grades; commit.

    If student doesn't exist: raise LookupError
    """
    student = Student.query.get(student_id)
    if not student:
        raise LookupError("student not found")

    db.session.delete(student)
    db.session.commit()

def delete_grade(grade_id: int) -> None:
    """TODO: Delete a grade by id; commit.

    If grade doesn't exist: raise LookupError
    """
    grade = Grade.query.get(grade_id)
    if not grade:
        raise LookupError("grade not found")

    db.session.delete(grade)
    db.session.commit()


# ===== FILTERING & FILTERING WITH AGGREGATION =====

def students_with_average_above(threshold: float) -> list[Student]:
    """TODO: Return students whose average percent is above threshold.

    List should be ordered by average percent descending.
    percent per grade = score / assignment.max_points * 100
    """
    subquery = (
        db.session.query(
            Grade.student_id.label("student_id"),
            func.avg((Grade.score / Assignment.max_points) * 100).label("avg_percent")
        )
        .join(Assignment, Grade.assignment_id == Assignment.id)
        .group_by(Grade.student_id)
        .subquery()
    )

    rows = (
        db.session.query(Student)
        .join(subquery, Student.id == subquery.c.student_id)
        .filter(subquery.c.avg_percent > threshold)
        .order_by(subquery.c.avg_percent.desc())
        .all()
    )

    return rows

def assignments_without_grades() -> list[Assignment]:
    """TODO: Return assignments that have no grades yet, ordered by title."""
    return (
        Assignment.query
        .outerjoin(Grade)
        .filter(Grade.id.is_(None))
        .order_by(Assignment.title.asc())
        .all()
    )

def top_scorer_on_assignment(assignment_id: int) -> Optional[Student]:
    """TODO: Return the Student with the highest score on an assignment.

    If assignment doesn't exist: raise LookupError
    If no grades on assignment: return None
    If tie (multiple students with same high score): return any one
    """
    assignment = Assignment.query.get(assignment_id)
    if not assignment:
        raise LookupError("assignment not found")

    top_grade = (
        Grade.query
        .filter(Grade.assignment_id == assignment_id)
        .order_by(Grade.score.desc())
        .first()
    )

    if not top_grade:
        return None

    return top_grade.student
