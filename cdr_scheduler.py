"""Run the scheduler process."""

import logging
import os
import sys
import cdr
from cdrapi import db
from cdrapi.settings import Tier

# Things we need to do before loading the other modules.
class Config:
    TIER = Tier()
    SENDER = "cdr@cancer.gov"

    @classmethod
    def init(cls):
        logging.basicConfig(
            filename="%s/scheduler.log" % cdr.DEFAULT_LOGDIR,
            format="[%(process)s/%(thread)s] " + cdr.Logging.FORMAT,
            level=logging.DEBUG
        )
        try:
            query = db.Query("usr e", "e.email")
            query.join("grp_usr u", "u.usr = e.id")
            query.join("grp g", "g.id = u.grp")
            query.where("g.name = 'Developers Notification'")
            rows = query.execute().fetchall()
            cls.RECIPS = [row[0] for row in rows if row[0]]
        except:
            logging.exception("database unavailable - exiting")
            sys.exit(1)
        if cls.RECIPS:
            try:
                subject = f"[{cls.TIER.name}] CDR Scheduler service restarted"
                opts = dict(subject=subject)
                message = cdr.EmailMessage(cls.SENDER, cls.RECIPS, **opts)
                message.send()
            except:
                pass
Config.init()

# Now bring in the other modules we need.
import tornado
from sqlalchemy import event
from sqlalchemy.pool import Pool
from ndscheduler import settings
from ndscheduler.server.handlers import audit_logs
from ndscheduler.server.handlers import executions
from ndscheduler.server.handlers import index
from ndscheduler.server.handlers import jobs
from ndscheduler.server import server

class CDRScheduler(server.SchedulerServer):

    STATIC_DIR = r"D:\Inetpub\wwwroot\cgi-bin\scheduler\static"

    def __init__(self, scheduler_instance):
        """Launch a web server to handle administering jobs.

        Unfortunately, the ndscheduler constructor does not
        expose the ability to control whether the tornado
        module can restart the scheduler when files change,
        so we have to override the constructor completely.
        That means that we'll lose any bug fixes or enhancements
        introduced later by ndscheduler in the constructor,
        and could even mean that they could do something which
        could break our constructor. We'll try to give them a
        patch which solves the problem by exposing the ability
        to pass in custom tornado settings.
        """

        self.scheduler_manager = scheduler_instance

        self.tornado_settings = dict(
            debug=settings.DEBUG,
            static_path=self.STATIC_DIR,
            template_path=self.STATIC_DIR,
            scheduler_manager=self.scheduler_manager,
            autoreload=False
        )

        # Setup server
        URLS = [
            # Index page
            (r"/", index.Handler),

            # APIs
            (r"/api/%s/jobs" % self.VERSION, jobs.Handler),
            (r"/api/%s/jobs/(.*)" % self.VERSION, jobs.Handler),
            (r"/api/%s/executions" % self.VERSION, executions.Handler),
            (r"/api/%s/executions/(.*)" % self.VERSION, executions.Handler),
            (r"/api/%s/logs" % self.VERSION, audit_logs.Handler),
        ]
        self.application = tornado.web.Application(URLS,
                                                   **self.tornado_settings)

    def start_scheduler(self):
        """
        Clean up outstanding failed jobs and start the scheduler.
        """

        settings_module = os.environ["NDSCHEDULER_SETTINGS_MODULE"]
        logging.info("sys.path: %s", sys.path)
        logging.info("directory: %s", os.getcwd())
        logging.info("PYTHONPATH: %s", os.environ["PYTHONPATH"])
        logging.info("SETTINGS MODULE: %s", settings_module)
        logging.info("STATIC DIR: %s", self.STATIC_DIR)
        self.fix_zombies()
        server.SchedulerServer.start_scheduler(self)

    def post_scheduler_stop(self):
        """
        Add our own logging to find out when the scheduler is stopped.
        """

        logging.info("CDR scheduler stopped")

    @staticmethod
    def fix_zombies():
        """
        Mark zombie execution runs as FAILED.

        From JIRA ticket OCECDR-4064:
        When the scheduler's python process is terminated, jobs which
        were running at the time remain marked as "Running."
        Change the status to "Failed."

        There is no API call in ndscheduler for fetching the execution
        IDs for executions in a given state. Instead the get_executions()
        method of the datastore object returns all executions whose
        scheduled_time value falls within a specified range. Since
        we don't have any way of knowing exactly which date range to
        use, and I'd rather not have it contstruct an Execution object
        for every run of every task of every job for the entire life
        of the system, we use the datastore's engine to run a query
        we build ourselves using sqlalchemy. We do use the datastore
        object to set the new FAILED state, though.
        """

        from ndscheduler import constants
        from ndscheduler.core import scheduler_manager
        from ndscheduler.core.datastore import tables
        from sqlalchemy import select
        failed = constants.EXECUTION_STATUS_FAILED
        running = constants.EXECUTION_STATUS_RUNNING
        desc = "marking zombie run as failed"
        eid_col = tables.EXECUTIONS.c.eid
        state_col = tables.EXECUTIONS.c.state
        scheduler = scheduler_manager.SchedulerManager.get_instance()
        datastore = scheduler.get_datastore()
        query = select([eid_col]).where(state_col == running)
        eids = [row.eid for row in datastore.engine.execute(query)]
        logging.info("found %d zombie executions", len(eids))
        for eid in eids:
            print(eid, (repr(eid)))
            logging.info("marking zombie %s as FAILED", eid)
            datastore.update_execution(eid, state=failed, description=desc)

    @staticmethod
    @event.listens_for(Pool, "checkout")
    def db_ping(conn, record, proxy):
        """
        Hook into the sqlalchemy layer to detect lost db connections.

        We need to exit when that happens so the service manager can
        keep trying to restart us until the database access is restored.
        """

        try:
            #logging.info("db_ping")
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        except Exception as e:
            logging.exception("database failure - exiting")
            if Config.RECIPS:
                try:
                    subject = f"[{Config.TIER.name}] CDR Scheduler DB failure"
                    opts = dict(subject=subject, body=str(e))
                    args = Config.SENDER, Config.RECIPS
                    message = cdr.EmailMessage(*args, **opts)
                except:
                    pass
            sys.exit(1)

if __name__ == "__main__":
    CDRScheduler.run()
