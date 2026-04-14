from .extensions import db


class Student(db.Model):
    __tablename__ = "students"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    grades = db.relationship(
        "Grade",
        back_populates="student",
        cascade="all, delete-orphan"
    )

    def to_dict(self):
        data = {
            "id": self.id,
            "name": self.name,
            "email": self.email,
        }
        return data


class Assignment(db.Model):
    __tablename__ = "assignments"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    max_score = db.Column(db.Integer, nullable=False)
    due_date = db.Column(db.Date, nullable=True)

    grades = db.relationship(
        "Grade",
        back_populates="assignment",
        cascade="all, delete-orphan"
    )

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "max_score": self.max_score,
            "due_date": self.due_date.isoformat() if self.due_date else None,
        }


class Grade(db.Model):
    __tablename__ = "grades"

    id = db.Column(db.Integer, primary_key=True)
    score = db.Column(db.Integer, nullable=False)
    comment = db.Column(db.String(200), nullable=True)

    student_id = db.Column(db.Integer, db.ForeignKey("students.id"), nullable=False)
    assignment_id = db.Column(db.Integer, db.ForeignKey("assignments.id"), nullable=False)

    student = db.relationship("Student", back_populates="grades")
    assignment = db.relationship("Assignment", back_populates="grades")

    def to_dict(self):
        return {
            "id": self.id,
            "score": self.score,
            "student_id": self.student_id,
            "assignment_id": self.assignment_id,
            "comment": self.comment,
        }