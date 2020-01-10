from cdr import Logging
from cdrapi import db
from datetime import datetime
from importlib import import_module
from json import loads
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep


class Control:
    SLEEP = 10
    DELETE = "DELETE FROM scheduled_job WHERE id = ?"
    SELECT = "SELECT * FROM scheduled_job"

    @property
    def conn(self):
        if not hasattr(self, "_conn"):
            self._conn = db.connect()
        return self._conn
    @property
    def jobs(self):
        if not hasattr(self, "_jobs"):
            self._jobs = {}
        return self._jobs
    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            self._logger = Logging.get_logger("cdr-scheduler")
        return self._logger

    @property
    def scheduler(self):
        if not hasattr(self, "_scheduler"):
            self._scheduler = BackgroundScheduler(timezone="US/Eastern")
        return self._scheduler
    @property
    def stopped(self):
        if not hasattr(self, "_stopped"):
            self._stopped = False
        return self._stopped

    @stopped.setter
    def stopped(self, value):
        self._stopped = value

    def run(self):
        self.logger.info("*" * 50)
        self.logger.info("CDR scheduler started")
        self.__load_jobs()
        self.scheduler.start()
        while not self.stopped:
            try:
                sleep(self.SLEEP)
                self.__refresh_jobs()
            except (KeyboardInterrupt, SystemExit):
                self.stop()
        self.logger.info("CDR scheduler stopped")
        self.scheduler.shutdown()

    def stop(self):
        self.stopped = True

    def delete_job(self, job_id):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(self.DELETE, job_id)
                self.conn.commit()
        except Exception:
            self.logger.exception("Failure deleting job %s", job_id)

    def __load_jobs(self):
        start = datetime.now()
        with self.conn.cursor() as cursor:
            for row in cursor.execute(self.SELECT).fetchall():
                try:
                    job = self.Job(self, row)
                    job.register()
                except Exception as e:
                    self.logger.exception("malformed job %s", tuple(row))
        elapsed = datetime.now() - start
        self.logger.info("registered %d jobs in %s", len(self.jobs), elapsed)

    def __refresh_jobs(self):
        start = datetime.now()
        found = set()
        with self.conn.cursor() as cursor:
            for row in cursor.execute(self.SELECT).fetchall():
                try:
                    job = self.Job(self, row)
                    found.add(job.id)
                    job.register()
                except Exception as e:
                    self.logger.exception("malformed job %s", tuple(row))
        for id in set(self.jobs) - found:
            job = self.jobs[id]
            self.logger.info("Job %r dropped", job.name)
            self.scheduler.remove_job(id)
            del self.jobs[id]
        elapsed = datetime.now() - start
        self.logger.debug("jobs refreshed in %s", elapsed)


    class Job:
        def __init__(self, control, row):
            self.__control = control
            self.__row = row

        def __eq__(self, other):
            return tuple(self.__row) == tuple(other.__row)

        def __str__(self):
            return str(tuple(self.__row))

        @property
        def enabled(self):
            return self.__row.enabled

        @property
        def id(self):
            return self.__row.id

        @property
        def job_class(self):
            return self.__row.job_class

        @property
        def logger(self):
            return self.__control.logger

        @property
        def name(self):
            return self.__row.name

        @property
        def opts(self):
            if not hasattr(self, "_opts"):
                self._opts = loads(self.__row.opts)
            return self._opts

        @property
        def schedule(self):
            if not hasattr(self, "_schedule"):
                self._schedule = self.__row.schedule
                if self._schedule:
                    self._schedule = loads(self._schedule)
            return self._schedule

        def register(self):
            old = self.__control.jobs.get(self.id)
            scheduler = self.__control.scheduler
            if old:
                if self == old:
                    return
                if not self.schedule:
                    self.logger.info("Removed %r schedule", old.name)
                    del self.__control.jobs[self.id]
                    scheduler.remove_job(self.id)
                    if self.enabled:
                        self.logger.info("Manual run of %r", self.name)
                        scheduler.add_job(self.run, name=self.name)
                        self.__control.delete_job(self.id)
                else:
                    self.logger.info("Modified %r registration", old.name)
                    opts = dict(func=self.run, name=self.name)
                    self.__control.jobs[self.id] = self
                    scheduler.modify_job(self.id, **opts)
                    if old.schedule != self.schedule:
                        opts = dict(self.schedule)
                        opts["trigger"] = "cron"
                        scheduler.reschedule_job(self.id, **opts)
                    if not old.enabled and self.enabled:
                        scheduler.resume_job(self.id)
                    elif not self.enabled and old.enabled:
                        scheduler.pause_job(self.id)
            elif self.schedule:
                self.logger.info("Registered %r", self.name)
                opts = dict(self.schedule)
                opts["id"] = self.id
                opts["name"] = self.name
                if not self.enabled:
                    opts["next_run_time"] = None
                scheduler.add_job(self.run, "cron", **opts)
                self.__control.jobs[self.id] = self
            elif self.enabled:
                self.logger.info("Unscheduled run of %r", self.name)
                scheduler.add_job(self.run, name=self.name)
                self.__control.delete_job(self.id)

        def run(self):
            try:
                start = datetime.now()
                module_name, class_name = self.job_class.split(".")
                module = import_module(f"jobs.{module_name}")
                args = self.__control, self.name
                getattr(module, class_name)(*args, **self.opts).run()
                args = start, datetime.now() - start, self.name
                self.logger.info("Job started %s, elapsed %s (%s)", *args)
            except Exception:
                self.logger.exception("%s started %s", self.name, start)


control = Control()
try:
    control.run()
except Exception as e:
    control.logger.exception("Failure")
    print(e)
