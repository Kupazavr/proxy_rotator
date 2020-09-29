from abc import ABC, abstractmethod
from abc import ABC
import os
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, FLOAT, inspect
from contextlib import contextmanager


class DB(ABC):
    pass


class SqliteDB(DB, ABC):
    db_folder_name = 'sqlite_dbs'

    def __init__(self, db_name, metadata):
        self.db_name = db_name
        self._metadata = metadata
        self._create_db_directory()
        self._client = self._connect_to_db(self.db_name)
        self._metadata.bind = self._client
        self._create_tables()
        self._session = self._get_sqlite_session()()

    def _connect_to_db(self, db_name):
        client = create_engine(f'sqlite:///{self.db_folder_name}/{db_name}.db')
        return client

    def _create_db_directory(self):
        if not os.path.exists(self.db_folder_name):
            os.makedirs(self.db_folder_name)

    def _create_tables(self):
        self._metadata.create_all(self._client)
        return

    def _get_sqlite_session(self):
        session = sessionmaker()
        session.configure(bind=self._client)
        return session

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        try:
            yield self._session
            self._session.commit()
        except:
            self._session.rollback()
            raise
        finally:
            self._session.close()
