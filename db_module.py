import logging
from sqlalchemy import create_engine, Column, String, ForeignKey, Table
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
from sqlalchemy.exc import NoResultFound

Base = declarative_base()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 1. Tasks Table
class Task(Base):
    __tablename__ = 'tasks'

    task_id_yandex = Column(String, primary_key=True)
    task_id_gandiva = Column(String, nullable=False)

# 2. Users Table
class User(Base):
    __tablename__ = 'users'

    yandex_user_id = Column(String, primary_key=True)
    gandiva_user_id = Column(String, nullable=False)

    departments = relationship("UserDepartment", back_populates="user")

# 3. Departments Table
class Department(Base):
    __tablename__ = 'departments'

    department_name = Column(String, primary_key=True)

    user_departments = relationship("UserDepartment", back_populates="department")

# 4. UserDepartments Table (Junction Table)
class UserDepartment(Base):
    __tablename__ = 'user_departments'

    yandex_user_id = Column(String, ForeignKey('users.yandex_user_id'), primary_key=True)
    department_name = Column(String, ForeignKey('departments.department_name'), primary_key=True)

    user = relationship("User", back_populates="departments")
    department = relationship("Department", back_populates="user_departments")


# Creating the Database and Tables
def create_database(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


# Session management
def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()



# Functions here
def find_task_by_gandiva_id(session: Session, task_id_gandiva: str):
    """
    Find a task in the database by its Gandiva ID.

    :param session: SQLAlchemy session object.
    :param task_id_gandiva: The Gandiva task ID to search for.
    :return: The Task object if found, otherwise None.
    """
    try:
        task = session.query(Task).filter_by(task_id_gandiva=task_id_gandiva).one()
        return task
    except NoResultFound:
        return None

def find_task_by_yandex_id(session: Session, task_id_yandex: str):
    """
    Find a task in the database by its Yandex ID.

    :param session: SQLAlchemy session object.
    :param task_id_yandex: The Yandex task ID to search for.
    :return: The Task object if found, otherwise None.
    """
    try:
        task = session.query(Task).filter_by(task_id_yandex=task_id_yandex).one()
        return task
    except NoResultFound:
        return None


def update_database_schema(engine):
    """Drop the table if it exists (this will remove all data!!!)"""
    Base.metadata.drop_all(engine)
    # Recreate the tables with the new schema
    Base.metadata.create_all(engine)

def add_tasks_to_db(session: Session, tasks: list):
    """
    Add a list of tasks to the database. Each task should have the structure:
    {'key': <task_id_yandex>, 'unique': <task_id_gandiva>}

    :param session: SQLAlchemy session object.
    :param tasks: A list of task dictionaries where 'key' is task_id_yandex and 'unique' is task_id_gandiva.
    :return: None
    """
    for task_data in tasks:
        task_id_yandex = task_data['key']
        task_id_gandiva = task_data['unique']

        # Check if the task already exists by task_id_yandex
        existing_task = session.query(Task).filter_by(task_id_yandex=task_id_yandex).one_or_none()

        if not existing_task:
            # If the task doesn't exist, create a new task and add it to the session
            new_task = Task(task_id_yandex=task_id_yandex, task_id_gandiva=task_id_gandiva)
            session.add(new_task)
            logging.info(f"Task {task_id_yandex} added to the database.")
        else:
            logging.info(f"Task {task_id_yandex} already exists in the database.")

    # Commit the changes to the database
    session.commit()
    logging.info("Tasks committed to the database.")