"""Settings to override default settings."""

import logging
from tornado.ioloop import PollIOLoop
from tornado.platform.select import SelectIOLoop, _Select
from util.cdr_connection_info import CDRDBConnectionInfo

#
# Override settings
#
DEBUG = True
TIMEZONE = "America/New_York"
HTTP_PORT = 8888
HTTP_ADDRESS = "127.0.0.1"

# Limit a configured job to only one instance running at a time.
JOB_MAX_INSTANCES = 1

# List of packages containing job classes. (Relative to the main
# scheduler script.)
JOB_CLASS_PACKAGES = ["jobs"]

# Package containing task classes.  This is a single package, stored
# relative to the overaall scheduler script.
TASK_CLASS_PACKAGE = "tasks"

# Setup for MS SQL Server
#
# Rather than duplicate the database configuration information, use
# CDRDBConnectionInfo to retrieve the same database information used
# by the rest of the CDR.
#
_dbInfo = CDRDBConnectionInfo()
DATABASE_CLASS = "core.datastore.providers.mssqlserver.DatastoreMSSqlServer"
DATABASE_CONFIG_DICT = {
    "user": _dbInfo.Username,
    "password": _dbInfo.Password,
    "hostname": _dbInfo.Server,
    "port": _dbInfo.Port,
    "database": _dbInfo.Database
}

#----------------------------------------------------------------------
# Plug in our own IOLoop object so we can get some more logging.
# This is massively scaled back from the version used for tracking
# down failures of the scheduler. In that version, the start()
# method was completely replaced, with tons of debugging logging.
#----------------------------------------------------------------------
class CDRSelect(_Select):
    def poll(self, timeout):
        logging.info("poll(%s seconds)", timeout)
        return _Select.poll(self, timeout)
class CDRIOLoop(SelectIOLoop):
    def __init__(self):
        self.install()
        logging.info("custom ioloop installed")
    def initialize(self, **kwargs):
        PollIOLoop.initialize(self, impl=CDRSelect(), **kwargs)
IO_LOOP = CDRIOLoop()
