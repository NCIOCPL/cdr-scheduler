"""Represents Microsoft SQL Server datastore."""

from ndscheduler import settings
from ndscheduler.core.datastore.providers import base


class DatastoreMSSqlServer(base.DatastoreBase):
    """
        DataStrore provider to allow the NDScheduler package
        to connect to Microsoft SQL Server.
        
        Requires the settings file to include a DATABASE_CONFIG_DICT
        structure containing the fields:
            user     - login userid
            password - login password
            hostname - database server
            port     - port number
            database - database name.
    """

    @classmethod
    def get_db_url(cls):
        """
        Returns a SQLAlchemy database URL as described at
        http://docs.sqlalchemy.org/en/latest/core/engines.html#microsoft-sql-server
        
        DATABASE_CONFIG_DICT = {
            'user': 'myuser',
            'password': 'password',
            'hostname': 'mydb.domain.com',
            'port': 5432,
            'database': 'mydatabase'
        }

        :return: database url
        :rtype: str
        """

        return 'mssql+pymssql://%s:%s@%s:%d/%s' % (
            settings.DATABASE_CONFIG_DICT['user'],
            settings.DATABASE_CONFIG_DICT['password'],
            settings.DATABASE_CONFIG_DICT['hostname'],
            settings.DATABASE_CONFIG_DICT['port'],
            settings.DATABASE_CONFIG_DICT['database'])
