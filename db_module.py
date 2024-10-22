import logging
from sqlalchemy import create_engine, Column, String, ForeignKey, Table, func, text
from sqlalchemy.orm import relationship, sessionmaker, Session, DeclarativeBase
from sqlalchemy.exc import NoResultFound, IntegrityError
from yandex_api import yc
import utils

DB_URL: str|None = None

def set_db_url(db_url):
    global DB_URL
    DB_URL = db_url

class Base(DeclarativeBase):
    pass


class Task(Base):
    __tablename__ = 'tasks'

    task_id_yandex = Column(String, primary_key=True)
    task_id_gandiva = Column(String, nullable=False)


class User(Base):
    __tablename__ = 'users'

    yandex_user_id = Column(String, primary_key=True)
    gandiva_user_id = Column(String, nullable=True)

    # Relationship with departments (one-to-many)
    departments = relationship("Department", back_populates="analyst")


class Department(Base):
    __tablename__ = 'departments'

    department_name = Column(String, primary_key=True)
    nd = Column(String)
    yandex_user_id = Column(String, ForeignKey('users.yandex_user_id'), nullable=False)  # Analyst for the department

    # Relationship with user (analyst)
    analyst = relationship("User", back_populates="departments")


def create_database(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine

def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

def get_db_session():
    db_engine = create_database(DB_URL)
    db_session = get_session(db_engine)
    return db_session




def get_task_by_gandiva_id(session: Session, g_task_id: str):
    try:
        task = session.query(Task).filter_by(task_id_gandiva=g_task_id).one_or_none()
        return task
    except NoResultFound:
        return None

def get_task_by_yandex_id(session: Session, task_id_yandex: str):
    try:
        task = session.query(Task).filter_by(task_id_yandex=task_id_yandex).one()
        return task
    except NoResultFound:
        return None


def update_database_schema(engine):
    """ Drop the table if it exists (this will remove all data!!!) """
    Base.metadata.drop_all(engine)
    # Recreate the tables with the new schema
    Base.metadata.create_all(engine)

def add_task_no_commit(session: Session, y_task: dict):
    """ Add a single task to the database or update it if it exists. """
    y_task_id         = y_task.get('key')
    g_task_id         = y_task.get(yc.fid_gandiva_task_id)
    
    if not g_task_id:
        logging.debug(f"No Gandiva ID found for Yandex task {y_task_id}, skipping.")
        return
    # Check if the task already exists by task_id_yandex
    existing_task = session.query(Task).filter_by(task_id_yandex=y_task_id).one_or_none()

    if not existing_task:
        # If the task doesn't exist, create a new task and add it to the session
        new_task = Task(task_id_yandex=y_task_id, task_id_gandiva=g_task_id)
        session.add(new_task)
        logging.info(f"Task {y_task_id} added to the database.")
    else:
        logging.debug(f"Task {y_task_id} already exists in the database.")

def add_tasks(session: Session, y_tasks: list):
    """ Add a list of tasks to the database by calling `add_task` for each task. """
    total_tasks = 0

    for y_task in y_tasks:
        add_task_no_commit(session, y_task)  # Call the add_task function for each task
        total_tasks += 1

    # Commit the changes to the database
    session.commit()
    logging.info(f"{total_tasks} tasks added/updated in the database.")

def add_user_department_nd_mapping(session: Session, department_user_mapping: dict):
    """ Adds or updates the user and department relationship in the database, considering both the department and НД fields. """
    total_entries = 0
    skipped_entries = 0

    for (department_name, nd), y_user_id in department_user_mapping.items():
        department_name = str(department_name)
        nd = str(nd)
        y_user_id = str(y_user_id)
        # Normalize department_name
        department_name = utils.normalize_department_name(department_name)
        
        # Check if the user exists in the database
        user = session.query(User).filter_by(yandex_user_id=y_user_id).one_or_none()

        if not user:
            # If the user doesn't exist, create and add a new user
            user = User(yandex_user_id=y_user_id)
            session.add(user)
            logging.info(f"User {y_user_id} added to the database.")

        # Check if the department exists in the database
        department = session.query(Department).filter_by(department_name=department_name).one_or_none()

        if not department:
            # If the department doesn't exist, create and add a new department with the НД field
            department = Department(department_name=department_name, nd=nd, yandex_user_id=y_user_id)
            session.add(department)
            logging.info(f"Department {department_name} with НД {nd} added to the database and assigned to user {y_user_id}.")
            total_entries += 1
            continue
        
        # Check if both the department's ND and yandex_user_id match the incoming data
        if str(department.nd) == nd and str(department.yandex_user_id) == y_user_id:
            # If the department is already up to date, skip the update
            logging.debug(f"Department {department_name} is already up-to-date with user {y_user_id} and НД {nd}. Skipping update.")
            skipped_entries += 1
        else:
            # Update department's ND field and user if needed
            department.nd = Column(nd)
            department.yandex_user_id = Column(y_user_id)
            logging.info(f"Department {department_name} updated with НД {nd} and assigned to user {y_user_id}.")
            total_entries += 1

    session.commit()  # Commit all changes at once
    logging.info(f"{total_entries} user-department mappings added/updated in the database. {skipped_entries} entries skipped.")

    return bool(total_entries)


def get_user_id_by_department(session: Session, department_name: str) -> str|None:
    """ Retrieves the user ID associated with the given department from the database. """
    try:
        department_name = utils.normalize_department_name(department_name)

        department = session.query(Department).filter_by(department_name=department_name).one_or_none()

        if not department:
            logging.warning(f"No user found for department: {department_name}")
            return None
        return str(department.yandex_user_id)
    
    except Exception as e:
        logging.error(f"Error fetching user ID for department {department_name}: {e}")
        return None

def add_or_update_user(session: Session, user_data: dict):
    """ Adds new users or updates existing ones in the database. """
    updates_count = 0
    for y_user_id, g_user_id in user_data.items():
        try:
            # Check if the user exists by yandex_user_id
            user = session.query(User).filter_by(yandex_user_id=str(y_user_id)).one_or_none()

            if user:
                # If the user exists but gandiva_user_id is empty or None, update it
                if user.gandiva_user_id is not None:
                    logging.debug(f"User {y_user_id} already exists with Gandiva ID {user.gandiva_user_id}. No update required.")
                    continue
                user.gandiva_user_id = Column(str(g_user_id))
                session.commit()
                logging.info(f"Updated user {y_user_id} with Gandiva ID {g_user_id}.")
                updates_count += 1
                continue
            # If the user doesn't exist, create a new user
            new_user = User(yandex_user_id=str(y_user_id), gandiva_user_id=str(g_user_id))
            session.add(new_user)
            session.commit()
            logging.info(f"Added new user {y_user_id} with Gandiva ID {g_user_id}.")
            updates_count += 1

        except Exception as e:
            session.rollback()
            logging.error(f"Error adding or updating user {y_user_id}: {str(e)}")

    return bool(updates_count)

def add_or_update_task(session: Session, g_task_id: str, y_task_id: str) -> None:
    """ Adds a new task if it doesn't exist, otherwise updates the existing one. """
    try:
        # Check if the task already exists
        task = session.query(Task).filter_by(task_id_yandex=y_task_id).one_or_none()

        if task is None:
            new_task = Task(task_id_yandex=y_task_id, task_id_gandiva=g_task_id)
            session.add(new_task)
            session.commit()
            logging.info(f"Task {y_task_id} added to the database.")
            return

        # If the task exists, update the Gandiva task ID if needed
        if str(task.task_id_gandiva) != g_task_id:
            task.task_id_gandiva = Column(g_task_id)
            session.commit()
            logging.info(f"Task {y_task_id} updated in the database with Gandiva ID {g_task_id}.")
            return
        logging.debug(f"Task {y_task_id} already up-to-date.")

    except IntegrityError as e:
        logging.error(f"Error adding or updating task {y_task_id}: {e}")
        session.rollback()

def get_user_by_yandex_id(session: Session, y_user_id: str):
    """ Retrieves a user from the database by their Yandex user ID. """
    try:
        user = session.query(User).filter_by(yandex_user_id=str(y_user_id)).one_or_none()
        return user
    except NoResultFound:
        logging.warning(f"No user found with Yandex user ID {y_user_id}.")
        return None
    except Exception as e:
        logging.error(f"Error retrieving user by Yandex ID {y_user_id}: {str(e)}")
        return None

def get_user_by_gandiva_id(session: Session, g_user_id: str):
    """ Retrieves a user from the database by their Gandiva user ID. """
    try:
        user = session.query(User).filter_by(gandiva_user_id=str(g_user_id)).one_or_none()
        return user
    except NoResultFound:
        logging.debug(f"No user found with Gandiva user ID {g_user_id}.")
        return None
    except Exception as e:
        logging.error(f"Error retrieving user by Gandiva ID {g_user_id}: {str(e)}")
        return None


def get_nd_by_department_name(session: Session, department_name: str) -> str|None:
    """ Retrieves ND from the database by their department_name. """
    try:
        # Normalize department_name
        department_name = utils.normalize_department_name(department_name)

        department = session.query(Department).filter_by(department_name=department_name).one_or_none()
        if not department:
            return None
        return str(department.nd)
    except NoResultFound:
        logging.debug(f"No department found with department_name {department_name}.")
        return None
    except Exception as e:
        logging.error(f"Error retrieving department by department_name {department_name}: {str(e)}")
        return None

def convert_gandiva_observers_to_yandex_followers(
        session, gandiva_observers: list[str]) -> list[str]:
    """ Converts a list of Gandiva observer IDs to corresponding Yandex follower IDs. """
    yandex_followers = [
        str(user.yandex_user_id) for gandiva_id in gandiva_observers 
        if ((user := get_user_by_gandiva_id(session, str(gandiva_id)))
            and bool(user.yandex_user_id))
    ]

    return yandex_followers

def find_duplicate_gandiva_tasks(db_session: Session) -> None:
    """ Logs a warning for all duplicate task_id_gandiva entries in the 'tasks' table. """

    duplicates = (db_session.query(Task.task_id_gandiva, func.count(Task.task_id_gandiva).label('count'))
                  .group_by(Task.task_id_gandiva)
                  .having(func.count(Task.task_id_gandiva) > 1)
                  .all())

    for gandiva_id in duplicates:
        yandex_ids = db_session.query(Task.task_id_yandex).filter(Task.task_id_gandiva == gandiva_id).all()
        yandex_ids_list = [y[0] for y in yandex_ids]
        logging.warning(f"Duplicate task_id_gandiva: {gandiva_id}, associated task_id_yandex: {', '.join(yandex_ids_list)}")

def clean_department_names(db_session: Session):
    try:
        rows_affected = db_session.query(Department).filter(
            Department.department_name.like('% %') | Department.department_name.like('%  %')
        ).update({
            Department.department_name: func.trim(
                func.replace(func.replace(Department.department_name, ' ', ' '), '  ', ' ')
            )
        }, synchronize_session=False)
        
        db_session.commit()

        logging.info(f"Department names cleaned successfully. Rows affected: {rows_affected}")

    except Exception as e:
        db_session.rollback()
        logging.error(f"Error occurred: {e}")


def is_user_analyst(session: Session, yandex_user_id: str|None) -> bool:
    """
    Checks if the given Yandex user ID is listed as an analyst in any department.

    :param session: SQLAlchemy session object.
    :param yandex_user_id: The Yandex user ID to check.
    :return: True if the user is an analyst in any department, False otherwise.
    """
    if not yandex_user_id:
        logging.debug("Yandex user id of None was used!")
        return False
    try:
        # Check if the user is associated with any department in the departments table
        analyst_exists = session.query(Department).filter_by(yandex_user_id=yandex_user_id).count()

        if analyst_exists > 0:
            return True
        else:
            logging.debug(f"User {yandex_user_id} is not an analyst in any department.")
            return False

    except Exception as e:
        logging.error(f"Error checking if user {yandex_user_id} is an analyst: {e}")
        return False
