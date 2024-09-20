import logging
from sqlalchemy import create_engine, Column, String, ForeignKey, Table
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
from sqlalchemy.exc import NoResultFound, IntegrityError
import yandex_api

Base = declarative_base()

# 1. Tasks Table
class Task(Base):
    __tablename__ = 'tasks'

    task_id_yandex = Column(String, primary_key=True)
    task_id_gandiva = Column(String, nullable=False)

# 2. Users Table
class User(Base):
    __tablename__ = 'users'

    yandex_user_id = Column(String, primary_key=True)
    gandiva_user_id = Column(String, nullable=True)

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
def get_task_by_gandiva_id(session: Session, task_id_gandiva: str):
    """
    Find a task in the database by its Gandiva ID.

    :param session: SQLAlchemy session object.
    :param task_id_gandiva: The Gandiva task ID to search for.
    :return: The Task object if found, otherwise None.
    """
    try:
        task = session.query(Task).filter_by(task_id_gandiva=task_id_gandiva).one_or_none()
        return task
    except NoResultFound:
        return None

def get_task_by_yandex_id(session: Session, task_id_yandex: str):
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

def add_task_no_commit(session: Session, task_data: dict):
    """
    Add a single task to the database or update it if it exists.
    :param session: SQLAlchemy session object.
    :param task_data: Dictionary containing 'key' (task_id_yandex) and YA_FIELD_ID_GANDIVA_TASK_ID (task_id_gandiva).
    :return: None
    """
    task_id_yandex = task_data.get('key')
    task_id_gandiva = task_data.get(yandex_api.YA_FIELD_ID_GANDIVA_TASK_ID)
    
    if not task_id_gandiva:
        logging.warning(f"No Gandiva ID found for Yandex task {task_id_yandex}, skipping.")
        return

    # Check if the task already exists by task_id_yandex
    existing_task = session.query(Task).filter_by(task_id_yandex=task_id_yandex).one_or_none()

    if not existing_task:
        # If the task doesn't exist, create a new task and add it to the session
        new_task = Task(task_id_yandex=task_id_yandex, task_id_gandiva=task_id_gandiva)
        session.add(new_task)
        logging.info(f"Task {task_id_yandex} added to the database.")
    else:
        logging.debug(f"Task {task_id_yandex} already exists in the database.")

def add_tasks(session: Session, tasks: list):
    """
    Add a list of tasks to the database by calling `add_task` for each task.
    :param session: SQLAlchemy session object.
    :param tasks: A list of task dictionaries where 'key' is task_id_yandex and YA_FIELD_ID_GANDIVA_TASK_ID is task_id_gandiva.
    :return: None
    """
    total_tasks = 0

    for task_data in tasks:
        add_task_no_commit(session, task_data)  # Call the add_task function for each task
        total_tasks += 1

    # Commit the changes to the database
    session.commit()
    logging.info(f"{total_tasks} tasks added/updated in the database.")

def add_user_department_mapping(session: Session, department_user_mapping: dict):
    """
    Adds or updates the user and department relationship in the database.

    :param session: SQLAlchemy session object.
    :param department_user_mapping: A dictionary where keys are department names, and values are user IDs.
    """
    total_entries = 0

    for department_name, user_id in department_user_mapping.items():
        # Check if the user exists in the database
        user = session.query(User).filter_by(yandex_user_id=user_id).one_or_none()

        if not user:
            # If the user doesn't exist, create and add a new user
            user = User(yandex_user_id=user_id)
            session.add(user)
            logging.info(f"User {user_id} added to the database.")

        # Check if the department exists in the database
        department = session.query(Department).filter_by(department_name=department_name).one_or_none()

        if not department:
            # If the department doesn't exist, create and add a new department
            department = Department(department_name=department_name)
            session.add(department)
            logging.info(f"Department {department_name} added to the database.")

        # Check if the relationship between the user and department already exists
        user_department = session.query(UserDepartment).filter_by(
            yandex_user_id=user_id,
            department_name=department_name
        ).one_or_none()

        if not user_department:
            # If the relationship doesn't exist, create it
            user_department = UserDepartment(yandex_user_id=user_id, department_name=department_name)
            session.add(user_department)
            logging.info(f"User {user_id} assigned to department {department_name}.")

        total_entries += 1

    # Commit the changes to the database
    session.commit()
    logging.info(f"{total_entries} user-department mappings added to the database.")

def get_user_id_by_department(session: Session, department_name: str) -> str:
    """
    Retrieves the user ID associated with the given department from the database.

    :param session: SQLAlchemy session object.
    :param department_name: The name of the department to search for.
    :return: The user ID associated with the department, or None if no user is found.
    """
    try:
        # Query the UserDepartment table to find the user associated with the department
        user_department = session.query(UserDepartment).filter_by(department_name=department_name).one_or_none()

        if user_department:
            return user_department.yandex_user_id
        else:
            logging.warning(f"No user found for department: {department_name}")
            return None
    except Exception as e:
        logging.error(f"Error fetching user ID for department {department_name}: {e}")
        return None

def add_or_update_user(session: Session, user_data: dict):
    """
    Adds new users or updates existing ones in the database.

    :param session: SQLAlchemy session object.
    :param user_data: Dictionary where keys are yandex_user_id and values are gandiva_user_id.
                      Example: {2332300: 60, ...}
    """
    for yandex_user_id, gandiva_user_id in user_data.items():
        try:
            # Check if the user exists by yandex_user_id
            user = session.query(User).filter_by(yandex_user_id=str(yandex_user_id)).one_or_none()

            if user:
                # If the user exists but gandiva_user_id is empty or None, update it
                if not user.gandiva_user_id:
                    user.gandiva_user_id = str(gandiva_user_id)
                    session.commit()
                    logging.info(f"Updated user {yandex_user_id} with Gandiva ID {gandiva_user_id}.")
                else:
                    logging.debug(f"User {yandex_user_id} already exists with Gandiva ID {user.gandiva_user_id}. No update required.")
            else:
                # If the user doesn't exist, create a new user
                new_user = User(yandex_user_id=str(yandex_user_id), gandiva_user_id=str(gandiva_user_id))
                session.add(new_user)
                session.commit()
                logging.info(f"Added new user {yandex_user_id} with Gandiva ID {gandiva_user_id}.")

        except Exception as e:
            session.rollback()
            logging.error(f"Error adding or updating user {yandex_user_id}: {str(e)}")

def add_or_update_task(session: Session, g_task_id: str, y_task_id: str):
    """
    Adds a new task if it doesn't exist, otherwise updates the existing one.

    :param session: SQLAlchemy session object.
    :param task_id_gandiva: Gandiva task ID to insert/update.
    :param task_id_yandex: Yandex task ID to insert/update.
    """
    try:
        # Check if the task already exists
        existing_task = session.query(Task).filter_by(task_id_yandex=y_task_id).one_or_none()

        if existing_task:
            # If the task exists, update the Gandiva task ID if needed
            if existing_task.task_id_gandiva != g_task_id:
                existing_task.task_id_gandiva = g_task_id
                logging.info(f"Task {y_task_id} updated in the database with Gandiva ID {g_task_id}.")
            else:
                logging.debug(f"Task {y_task_id} already up-to-date.")
        else:
            # If the task doesn't exist, add a new one
            new_task = Task(task_id_yandex=y_task_id, task_id_gandiva=g_task_id)
            session.add(new_task)
            logging.info(f"Task {y_task_id} added to the database.")

        # Commit the changes
        session.commit()

    except IntegrityError as e:
        logging.error(f"Error adding or updating task {y_task_id}: {e}")
        session.rollback()

def get_user_by_yandex_id(session: Session, yandex_user_id: str):
    """
    Retrieves a user from the database by their Yandex user ID.

    :param session: SQLAlchemy session object.
    :param yandex_user_id: The Yandex user ID to search for.
    :return: The User object if found, otherwise None.
    """
    try:
        user = session.query(User).filter_by(yandex_user_id=str(yandex_user_id)).one_or_none()
        return user
    except NoResultFound:
        logging.warning(f"No user found with Yandex user ID {yandex_user_id}.")
        return None
    except Exception as e:
        logging.error(f"Error retrieving user by Yandex ID {yandex_user_id}: {str(e)}")
        return None

def get_user_by_gandiva_id(session: Session, gandiva_user_id: str):
    """
    Retrieves a user from the database by their Gandiva user ID.

    :param session: SQLAlchemy session object.
    :param gandiva_user_id: The Gandiva user ID to search for.
    :return: The User object if found, otherwise None.
    """
    try:
        user = session.query(User).filter_by(gandiva_user_id=str(gandiva_user_id)).one_or_none()
        return user
    except NoResultFound:
        logging.debug(f"No user found with Gandiva user ID {gandiva_user_id}.")
        return None
    except Exception as e:
        logging.error(f"Error retrieving user by Gandiva ID {gandiva_user_id}: {str(e)}")
        return None

def convert_gandiva_observers_to_yandex_followers(session, gandiva_observers: list[int]) -> list[int]:
    """
    Converts a list of Gandiva observer IDs to corresponding Yandex follower IDs.

    :param gandiva_observers: A list of Gandiva observer IDs.
    :param session: The SQLAlchemy session used for database queries.
    :return: A list of Yandex follower IDs.
    """
    yandex_followers = []

    for gandiva_id in gandiva_observers:
        user = get_user_by_gandiva_id(session, gandiva_id)
        if user and user.yandex_user_id:
            yandex_followers.append(user.yandex_user_id)

    return yandex_followers

if __name__ == '__main__':
    pass