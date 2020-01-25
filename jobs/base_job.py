"""Base class whose derived classes implement job functionality.
"""

import logging
from threading import Lock
from cdr import DEFAULT_LOGDIR
from cdrapi import db


class Job:

    LOGNAME = "scheduled-job"
    LOGGING_LOCK = Lock()
    LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

    def __init__(self, control, name, **opts):
        self.control = control
        self.name = name
        self.opts = opts

    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            with Job.LOGGING_LOCK:
                self._logger = logging.getLogger(self.LOGNAME)
                if not (self._logger.handlers):
                    self._logger.setLevel(logging.INFO)
                    path = f"{DEFAULT_LOGDIR}/{self.LOGNAME}.log"
                    handler = logging.FileHandler(path)
                    formatter = logging.Formatter(self.LOG_FORMAT)
                    handler.setFormatter(formatter)
                    self._logger.addHandler(handler)
        return self._logger

    def run(self):
        raise Exception("derived class must override run() method")

    @staticmethod
    def get_group_email_addresses(group_name="Developers Notification"):
        """
        Replacement for cdr.getEmailList() which does not exclude retired
        accounts.
        """
        query = db.Query("usr u", "u.email")
        query.join("grp_usr gu", "gu.usr = u.id")
        query.join("grp g", "g.id = gu.grp")
        query.where(query.Condition("g.name", group_name))
        query.where("u.expired IS NULL")
        return [row[0] for row in query.execute().fetchall() if row[0]]
