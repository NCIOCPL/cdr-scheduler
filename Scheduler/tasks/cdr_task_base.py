import logging
from core.exceptions import TaskException
import cdrdb2 as cdrdb

class CDRTask(object):
    """
    Base class for all tasks performed by a CDRJob subclass.
    """

    LOG_LEVELS = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "warn": logging.WARN,
        "warning": logging.WARNING,
        "critical": logging.CRITICAL,
        "error": logging.ERROR
    }
    "Map of log-level options to logging module constants."

    def __init__(self, jobParams, taskData):
        """
        jobParams: configuration document passed to the parent job.
        taskData: arguments passed to the task which were calculated by the job
            (e.g. via another task).
        """
        msg = "%s must be a dictionary object. Got %s instead."
        if not(isinstance(jobParams, dict)):
            type_name = type(jobParams).__name__
            raise TypeError(msg % ("jobParams", repr(type_name)))
        if not(isinstance(taskData, dict)):
            type_name = type(taskData).__name__
            raise TypeError(msg % ("taskData", repr(type_name)))

        self.jobParams = jobParams
        self.taskData = taskData
        self.logger = CDRTask.get_logger(jobParams)

    def Perform(self):
        """
        Performs the concrete task's work. Must be implmented in all
        CDRTask subclasses.
        """
        raise NotImplementedError('The Perform() method must be implemented in all CDRTask subclasses.')

    def get_required_param(self, name):
        if name in self.jobParams:
            return self.jobParams[name]
        else:
            raise TaskException("Required parameter '%s' not found." % (name,))

    def get_optional_param(self, name, default):
        if name in self.jobParams:
            return self.jobParams[name]
        else:
            return default

    @classmethod
    def get_logger(cls, params):
        log_level = params.get("log-level", "info")
        logger = logging.getLogger(__name__)
        logger.setLevel(cls.LOG_LEVELS.get(log_level, logging.INFO))
        return logger

    @staticmethod
    def get_group_email_addresses(group_name):
        """
        Replacement for cdr.getEmailList() which uses a DB API which
        does not do well in multi-threaded environments.
        """
        query = cdrdb.Query("usr u", "u.email")
        query.join("grp_usr gu", "gu.usr = u.id")
        query.join("grp g", "g.id = gu.grp")
        query.where(query.Condition("g.name", group_name))
        return [row[0] for row in query.execute().fetchall() if row[0]]
