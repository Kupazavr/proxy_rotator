from abc import ABC, abstractmethod
from typing import Union, List
import logging
from db import SqliteDB
from sqlalchemy import MetaData
from sqlalchemy import Table, MetaData, Column, Integer, String, ForeignKey, Float
from sqlalchemy.orm import mapper
from extractors import Proxy


class ProxyStorage(ABC):

    @abstractmethod
    def select_proxy(self) -> Union[Proxy, List[Proxy]]:
        '''
        Select proxy from storage returns list of proxy addresses if no arguments provided
        '''
        pass

    @abstractmethod
    def update_proxy(self, proxy: Proxy, **kwargs) -> None:
        pass

    @abstractmethod
    def delete_proxy(self, proxy: Proxy) -> None:
        pass

    @abstractmethod
    def insert_proxy(self, proxy: Proxy):
        pass

    @abstractmethod
    def wipe(self):
        """
        Delete all data from storage
        """
        pass


class ProxySqliteStorage(ProxyStorage, SqliteDB, ABC):
    _table_name = 'proxies'
    _db_name = 'proxies'

    def __init__(self):
        self._metadata = MetaData()
        self._map_proxies_to_sqlite()
        super(ProxySqliteStorage, self).__init__(self._db_name, self._metadata)

    def _map_proxies_to_sqlite(self):
        proxy_table = Table(self._table_name, self._metadata, *self._get_columns())
        mapper(Proxy, proxy_table)

    @property
    def types_mapper(self) -> dict:
        return {str: String,
                int: Integer,
                float: Float}

    def _get_columns(self) -> List[Column]:
        columns = [Column(Proxy.uniq_key, self.types_mapper[Proxy.uniq_key_type], primary_key=True)]
        for field_name, field_type in Proxy.mandatory_fields.items():
            columns.append(Column(field_name, self.types_mapper[field_type], default=None))
        return columns

    def select_proxy(self) -> Union[Proxy, List[Proxy]]:
        pass

    def update_proxy(self, proxy: Proxy, **kwargs) -> None:
        pass

    def delete_proxy(self, proxy: Proxy) -> None:
        pass

    def insert_proxy(self, proxy: Proxy):
        with self.session_scope() as session:
            session.add(proxy)
        return

    def wipe(self):
        self._metadata.tables[self._table_name].drop()
        self._create_tables()


if __name__ == '__main__':
    pss = ProxySqliteStorage()
    # pss.
    print('b')
