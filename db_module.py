import logging

# SQLAlchemy Imports
from sqlalchemy import create_engine, Column, String, ForeignKey, func, Engine
from sqlalchemy.orm import relationship, sessionmaker, Session, DeclarativeBase
from sqlalchemy.exc import NoResultFound, IntegrityError

# Project-Specific Modules
from common_utils import normalize_department_name


db_url: str = "sqlite:///project.db"


def set_db_url(
        url: str
) -> None:
    """
    Sets the global database URL.

    :param url: The database URL to set.
    :type url: str
    """
    global db_url
    db_url = url


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
    yandex_user_id = Column(String, ForeignKey(
        'users.yandex_user_id'), nullable=False)  # Analyst for the department

    # Relationship with user (analyst)
    analyst = relationship("User", back_populates="departments")


def create_database(
        db_url: str
) -> Engine:
    """
    Creates a database with the defined schema.

    :param db_url: The database URL to connect and create the schema.
    :type db_url: str
    :return: The SQLAlchemy engine connected to the database.
    :rtype: Engine
    """
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


def get_session(
        engine: Engine
) -> Session:
    """
    Creates a new SQLAlchemy session bound to the given engine.

    :param engine: The engine to bind to the session.
    :type engine: Engine
    :return: A new session object.
    :rtype: Session
    """

    new_session = sessionmaker(bind=engine)
    return new_session()


def get_db_session() -> Session:
    """
    Gets a new database session object.

    This function creates a new database connection and returns a session
    object bound to it. The session object is used to query the database.

    :return: A new session object.
    :rtype: Session
    """
    db_engine = create_database(db_url)
    db_session = get_session(db_engine)
    return db_session


def get_task_by_gandiva_id(
        session: Session,
        g_task_id: str
) -> Task | None:
    """
    Gets a task by its Gandiva ID.

    :param session: The database session to query the database with.
    :param g_task_id: The Gandiva ID of the task to retrieve.
    :return: The task with the given Gandiva ID or None if no such task exists.
    :rtype: Task | None
    """
    try:
        task = session.query(Task).filter_by(
            task_id_gandiva=g_task_id).one_or_none()
        return task
    except NoResultFound:
        return None


def get_task_by_yandex_id(
        session: Session,
        task_id_yandex: str
) -> Task | None:
    """
    Gets a task by its Yandex ID.

    :param session: The database session to query the database with.
    :param task_id_yandex: The Yandex ID of the task to retrieve.
    :return: The task with the given Yandex ID or None if no such task exists.
    :rtype: Task | None
    """
    try:
        task = session.query(Task).filter_by(
            task_id_yandex=task_id_yandex).one()
        return task
    except NoResultFound:
        return None


def update_database_schema(
        engine: Engine
) -> None:
    """ Drop the table if it exists (!ALL DATA WILL BE REMOVED!) """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def add_task_no_commit(
        session: Session,
        y_task: dict[str, str],
        fid_gandiva_task_id: str
) -> None:
    """ Add a single task to the database or update it if it exists. """
    y_task_id = y_task.get('key')
    g_task_id = y_task.get(fid_gandiva_task_id)

    if not g_task_id:
        logging.debug(
            f"No Gandiva ID found for Yandex task {y_task_id}, skipping.")
        return

    task_in_db = session.query(Task).filter_by(
        task_id_yandex=y_task_id).one_or_none()

    if not task_in_db:
        new_task = Task(task_id_yandex=y_task_id, task_id_gandiva=g_task_id)
        session.add(new_task)
        logging.info(f"Task {y_task_id} added to the database.")
        return

    logging.debug(f"Task {y_task_id} already exists in the database.")


def add_tasks(
        session: Session,
        y_tasks: list[dict[str, str]],
        fid_gandiva_task_id: str
) -> None:
    """ Add a list of tasks to the database by calling `add_task` for each task. """
    total_tasks = 0

    for y_task in y_tasks:
        add_task_no_commit(session, y_task, fid_gandiva_task_id)
        total_tasks += 1

    session.commit()
    logging.info(f"{total_tasks} tasks added/updated in the database.")


def add_user_department_nd_mapping(
        session: Session,
        department_user_mapping: dict[tuple[str, str], str]
) -> bool:
    """Adds or updates user-department mappings in the database
    based on department name and НД (ND)."""
    total_entries = 0
    skipped_entries = 0

    def add_user_if_not_exists(session: Session, y_user_id: str, user: User | None) -> None:
        if not user:
            session.add(User(yandex_user_id=y_user_id))
            logging.info(f"User {y_user_id} added to the database.")

    for (department_name, nd), y_user_id in department_user_mapping.items():
        department_name = normalize_department_name(str(department_name))
        nd = str(nd)
        y_user_id = str(y_user_id)

        user = session.query(User).filter_by(
            yandex_user_id=y_user_id).one_or_none()
        add_user_if_not_exists(session, y_user_id, user)

        department = (session.query(Department)
                      .filter_by(department_name=department_name)
                      .one_or_none())
        if not department:
            session.add(
                Department(department_name=department_name, nd=nd, yandex_user_id=y_user_id))
            logging.info(
                f"Department {department_name} with НД {nd} added for user {y_user_id}.")
            total_entries += 1
            continue

        data_was_changed = (
            str(department.nd) != nd
            or str(department.yandex_user_id) != y_user_id)

        if not data_was_changed:
            skipped_entries += 1
            logging.debug(f"Skipping update for department {department_name}.")
        else:
            department.nd = nd  # type: ignore
            department.yandex_user_id = y_user_id  # type: ignore
            logging.info(
                f"Department {department_name} updated with НД {nd} for user {y_user_id}.")
            total_entries += 1

    session.commit()
    logging.info(
        f"{total_entries} mappings added/updated. {skipped_entries} entries skipped.")

    return bool(total_entries)


def get_user_id_by_department(
        session: Session,
        department_name: str
) -> str | None:
    """ Retrieves the user ID associated with the given department from the database. """
    try:
        department_name = normalize_department_name(department_name)

        department = session.query(Department).filter_by(
            department_name=department_name).one_or_none()

        if not department:
            logging.warning(f"No user found for department: {department_name}")
            return None
        return str(department.yandex_user_id)

    except Exception as e:
        logging.error(
            f"Error fetching user ID for department {department_name}: {e}")
        return None


def add_or_update_user(
        session: Session,
        user_data: dict[str, str]
) -> bool:
    """ Adds new users or updates existing ones in the database. """
    updates_count = 0
    for y_user_id, g_user_id in user_data.items():
        try:
            user = session.query(User).filter_by(
                yandex_user_id=str(y_user_id)).one_or_none()

            if not user:
                create_user(session, updates_count, y_user_id, g_user_id)
                continue

            gandiva_user_id_exists = user.gandiva_user_id is not None
            if gandiva_user_id_exists:
                logging.debug(
                    f"User {y_user_id} already exists with Gandiva ID "
                    f"{user.gandiva_user_id}. No update required."
                )
                continue

            update_user(session, y_user_id, g_user_id, user)
            updates_count += 1

        except Exception as e:
            session.rollback()
            logging.error(
                f"Error adding or updating user {y_user_id}: {str(e)}")

    return updates_count > 0


def update_user(
        session: Session,
        y_user_id: str,
        g_user_id: str,
        user: User
) -> None:
    user.gandiva_user_id = Column(str(g_user_id))
    session.commit()
    logging.info(
        f"Updated user {y_user_id} with Gandiva ID {g_user_id}.")


def create_user(
        session: Session,
        updates_count: int,
        y_user_id: str,
        g_user_id: str
) -> None:
    """
    Adds a new user to the database if it doesn't exist yet.

    :param session: SQLAlchemy session
    :param updates_count: number of user updates so far
    :param y_user_id: ID of the user in Yandex Tracker
    :param g_user_id: ID of the user in Gandiva
    """
    new_user = User(yandex_user_id=str(y_user_id),
                    gandiva_user_id=str(g_user_id))
    session.add(new_user)
    session.commit()
    logging.info(
        f"Added new user {y_user_id} with Gandiva ID {g_user_id}.")
    updates_count += 1


def add_or_update_task(
        session: Session,
        g_task_id: str,
        y_task_id: str
) -> None:
    """ Adds a new task if it doesn't exist, otherwise updates the existing one. """
    try:

        task = session.query(Task).filter_by(
            task_id_yandex=y_task_id).one_or_none()
        if task is None:
            new_task = Task(task_id_yandex=y_task_id,
                            task_id_gandiva=g_task_id)
            session.add(new_task)
            session.commit()
            logging.info(f"Task {y_task_id} added to the database.")
            return

        task_exists = str(task.task_id_gandiva) != g_task_id
        if task_exists:
            update_task_gandiva_task_id(session, g_task_id, y_task_id, task)
            return

        logging.debug(f"Task {y_task_id} already up-to-date.")

    except IntegrityError as e:
        logging.error(f"Error adding or updating task {y_task_id}: {e}")
        session.rollback()


def update_task_gandiva_task_id(
        session: Session,
        g_task_id: str,
        y_task_id: str,
        task: Task
) -> None:
    """ Updates the Gandiva task ID in the database for a given task. """

    task.task_id_gandiva = Column(g_task_id)
    session.commit()
    logging.info(
        f"Task {y_task_id} updated in the database with Gandiva ID {g_task_id}.")


def get_user_by_yandex_id(
        session: Session,
        y_user_id: str
) -> User | None:
    """ Retrieves a user from the database by their Yandex user ID. """
    try:
        user = session.query(User).filter_by(
            yandex_user_id=str(y_user_id)).one_or_none()
        return user
    except Exception as e:
        logging.error(
            f"Error retrieving user by Yandex ID {y_user_id}: {str(e)}")
        return None


def get_user_by_gandiva_id(
        session: Session,
        g_user_id: str
) -> User | None:
    """ Retrieves a user from the database by their Gandiva user ID. """
    try:
        user = session.query(User).filter_by(
            gandiva_user_id=g_user_id).one_or_none()
        return user
    except Exception as e:
        logging.error(
            f"Error retrieving user by Gandiva ID {g_user_id}: {str(e)}")
        return None


def get_nd_by_department_name(
        session: Session,
        department_name: str
) -> str | None:
    """ Retrieves ND from the database by their department_name. """
    try:
        department_name = normalize_department_name(department_name)

        department = session.query(Department).filter_by(
            department_name=department_name).one_or_none()
        if not department:
            return None
        return str(department.nd)

    except Exception as e:
        logging.error(
            f"Error retrieving department by department_name {department_name}: {str(e)}")
        return None


def convert_gandiva_observers_to_yandex_followers(
        session: Session,
        gandiva_observers: list[str]
) -> list[str]:
    """ Converts a list of Gandiva observer IDs to corresponding Yandex follower IDs. """
    yandex_followers = [
        str(user.yandex_user_id) for gandiva_id in gandiva_observers
        if ((user := get_user_by_gandiva_id(session, gandiva_id))
            and bool(user.yandex_user_id))
    ]

    return yandex_followers


def find_duplicate_gandiva_tasks(
        db_session: Session
) -> None:
    """ Logs a warning for all duplicate task_id_gandiva entries in the 'tasks' table. """

    duplicates = (
        db_session
        .query(Task.task_id_gandiva, func.count(Task.task_id_gandiva).label('count'))
        .group_by(Task.task_id_gandiva)
        .having(func.count(Task.task_id_gandiva) > 1)
        .all()
    )

    for gandiva_id, _ in duplicates:
        yandex_ids = db_session.query(Task.task_id_yandex).filter(
            Task.task_id_gandiva == gandiva_id).all()
        yandex_ids_list = [str(y[0]) for y in yandex_ids]
        logging.warning(
            f"Duplicate task_id_gandiva: {gandiva_id}, associated "
            f"task_id_yandex: {', '.join(yandex_ids_list)}"
        )


def clean_department_names(
        db_session: Session
) -> None:
    try:
        rows_affected = db_session.query(Department).filter(
            Department.department_name.like(
                '% %') | Department.department_name.like('%  %')
        ).update({
            Department.department_name: func.trim(
                func.replace(func.replace(
                    Department.department_name, ' ', ' '), '  ', ' ')
            )
        }, synchronize_session=False)

        db_session.commit()

        logging.info(
            f"Department names cleaned successfully. Rows affected: {rows_affected}")

    except Exception as e:
        db_session.rollback()
        logging.error(f"Error occurred: {e}")


def is_user_analyst(
        session: Session,
        yandex_user_id: str
) -> bool:
    """
    Checks if the given Yandex user ID is listed as an analyst in any department.

    :param session: SQLAlchemy session object.
    :param yandex_user_id: The Yandex user ID to check.
    :return: True if the user is an analyst in any department, False otherwise.
    """
    if not yandex_user_id:
        logging.debug("Yandex user id of empty string was used!")
        return False
    try:
        analyst_exists = session.query(Department).filter_by(
            yandex_user_id=yandex_user_id).count()

        if analyst_exists > 0:
            return True
        else:
            logging.debug(
                f"User {yandex_user_id} is not an analyst in any department.")
            return False

    except Exception as e:
        logging.error(
            f"Error checking if user {yandex_user_id} is an analyst: {e}")
        return False
