"""Settings to override default settings."""

import logging
import logging.config
import logging.handlers

from util.cdr_connection_info import CDRDBConnectionInfo


#
# Override settings
#
DEBUG = True

HTTP_PORT = 8888
HTTP_ADDRESS = '127.0.0.1'


# Limit a configured job to only one instance running at a time.
JOB_MAX_INSTANCES = 1

# Configure logging
handler = logging.FileHandler("d:\\cdr\\Log\\scheduler.log")
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.ERROR)


# List of packages containing job classes. (Relative to the main scheduler script.)
JOB_CLASS_PACKAGES = ['jobs']

# Package containing task classes.  This is a single package, stored relative to the
# overaall scheduler script.
TASK_CLASS_PACKAGE = 'tasks'


# Setup for MS SQL Server
#
# Rather than duplicate the database configuration information, use
# CDRDBConnectionInfo to retrieve the same database information used
# by the rest of the CDR.
#
_dbInfo = CDRDBConnectionInfo()
DATABASE_CLASS = 'core.datastore.providers.mssqlserver.DatastoreMSSqlServer'
DATABASE_CONFIG_DICT = {
    'user': _dbInfo.Username,
    'password': _dbInfo.Password,
    'hostname': _dbInfo.Server,
    'port': _dbInfo.Port,
    'database': _dbInfo.Database
}
