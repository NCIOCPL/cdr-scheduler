"""Run scheduled CDR jobs.

Replaces the scheduler built with the `ndscheduler` package, which was
not very well maintained, and was beginning to present problems with
its dependencies on older versions of Python and other third-party
packages.

The jobs managed by this program are stored in the `scheduled_job`
database table with a name, unique ID, implementing class, optional
schedule using cron-like syntax, a flag indicating whether the job
is currently enabled, and parameter options fed to the job at runtime.
The controller stores this information for each job in a `Control.Job`
object (not to be confused with the implementing class, which is derived
from the `jobs.base_job.Job` base class).

If a job has a `schedule`, as is the case for most jobs, it is
registered with the scheduler, and its `enabled` flag is used to
control whether the job is paused or runnable.

If there is no `schedule` for a job its handling depends on whether
the `enabled` flag is set. If it is set the job is run once and its
row is dropped from the database table to prevent it from running
repeatedly. If the `enabled` flag is not set the job is ignored
(the row exists in the table only to be copied to a new row with
the `enabled` flag set when the user asks for a one-off run of a
job without a schedule.

For more information, see the documentation at the top of Scheduler.py in
https://github.com/NCIOCPL/cdr-admin/tree/master/Inetpub/wwwroot/cgi-bin/cdr.

To understand how the scheduler works, start by looking at the `run()`
method of the `Control` class (along with its `__load_jobs()` and
`__refresh_jobs()` methods). Then study the comments on the `register()`
methods of the `Control.Job` class. Finally, take a look at the `run()`
method of that class.

"""


from cdr import Logging
from cdrapi import db
from datetime import datetime
from importlib import import_module
from json import loads
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep


class Control:
    """Situation room for scheduled jobs."""

    SLEEP = 10
    DELETE = "DELETE FROM scheduled_job WHERE id = ?"
    SELECT = "SELECT * FROM scheduled_job"

    def run(self):
        """Top-level entry point.

        Load the jobs from the database table and register them with
        the scheduler. Then check in a loop for any changes which
        need to be registered. Exit the loop and shut down the
        scheduler when the `stopped` flag is set (by the _Restart
        Scheduler_ job).
        """

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
        """Break out of the run() loop so we can bounce the service."""
        self.stopped = True

    def delete_job(self, job_id):
        """Remove a row from the `scheduled_job` table.

        Pass:
            job_id - primary key for the row to be dropped
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(self.DELETE, job_id)
                self.conn.commit()
        except Exception:
            self.logger.exception("Failure deleting job %s", job_id)

    @property
    def conn(self):
        """Read/write access to the database."""

        if not hasattr(self, "_conn"):
            self._conn = db.connect()
        return self._conn

    @property
    def jobs(self):
        """Dictionary of `Control.Job` objects."""

        if not hasattr(self, "_jobs"):
            self._jobs = {}
        return self._jobs

    @property
    def logger(self):
        """Object for recording what we do."""

        if not hasattr(self, "_logger"):
            self._logger = Logging.get_logger("cdr-scheduler")
        return self._logger

    @property
    def scheduler(self):
        """Let the APScheduler package handle the scheduled jobs."""

        if not hasattr(self, "_scheduler"):
            self._scheduler = BackgroundScheduler(timezone="US/Eastern")
        return self._scheduler

    @property
    def stopped(self):
        """If `True` we're shutting down."""

        if not hasattr(self, "_stopped"):
            self._stopped = False
        return self._stopped

    @stopped.setter
    def stopped(self, value):
        """Allow the process to shut down so we can reboot the service."""
        self._stopped = value

    def __load_jobs(self):
        """Get the jobs from the DB and register them with the scheduler.

        Called by `run()` when we're starting up.

        Create a `Control.Job` object for each row in the `scheduled_job`
        database table and invoke that object's `register()` method to
        tell the scheduler about the job and plug itself into our
        `jobs` dictionary property.
        """

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
        """Handle any changes made to the job table since we last checked.

        Create fresh `Control.Job` objects for each row in the
        `scheduled_job` database table and tell the scheduler about
        any new jobs and changes to jobs it already knows about (by
        invoking the objects' `register()` method).

        Then drop and unregister any jobs whose database row has been
        removed from the table.
        """

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
        """Settings from the database table for this job.

        Objects of this class communicate with the APScheduler package
        which is responsible for determining when the job should be
        run. The job will also have a class derived from the base
        class `jobs.base_job.Job` which is used for implementing the
        functionality for that job, with its own `run()` method which
        is invoked when it is time for the job to execute.

        """

        def __init__(self, control, row):
            """Remember the caller's values.

            Pass:
                control - access to the database, logger and all jobs
                row - database table's values for this job
            """

            self.__control = control
            self.__row = row

        def __eq__(self, other):
            """Detect whether the job has changed."""
            return tuple(self.__row) == tuple(other.__row)

        def __str__(self):
            """Serialization of the job's values as a tuple."""
            return str(tuple(self.__row))

        @property
        def enabled(self):
            """True if the job can be executed."""
            return self.__row.enabled

        @property
        def id(self):
            """Unique ID for the job."""
            return self.__row.id

        @property
        def job_class(self):
            """String for the implementing class for the job.

            The format of the string is module_name.class_name.
            """

            return self.__row.job_class

        @property
        def logger(self):
            """Use the same logger as the controller."""
            return self.__control.logger

        @property
        def name(self):
            """Display name for the job."""
            return self.__row.name

        @property
        def opts(self):
            """Parameter options passed to the job."""

            if not hasattr(self, "_opts"):
                self._opts = loads(self.__row.opts)
            return self._opts

        @property
        def schedule(self):
            """Optional dictionary of cron-like scheduling values.

            The values are stored in the database as a serialized
            `json` string. For example, {"hour": "1", "minute": "15"}
            would run the job daily at 1:15 a.m.
            """

            if not hasattr(self, "_schedule"):
                self._schedule = self.__row.schedule
                if self._schedule:
                    self._schedule = loads(self._schedule)
            return self._schedule

        def register(self):
            """Remember the job, if appropriate.

            Affects both the scheduler's list of jobs and the controller's
            dictionary of jobs. For some situations, the method also
            removes the job's row from the database table.
            """

            scheduler = self.__control.scheduler
            old = self.__control.jobs.get(self.id)
            if old:

                # If nothing has changed, we're done here (most common case).
                if self == old:
                    return

                # If the schedule has been removed from a job which
                # had one, forget it.
                if not self.schedule:
                    self.logger.info("Removed %r schedule", old.name)
                    del self.__control.jobs[self.id]
                    scheduler.remove_job(self.id)

                    # Not a likely sequence, but if the user removes
                    # the job's schedule but leaves the `enabled`
                    # flag set, do a one-off execution of the job.
                    # Then drop the row from the database table so
                    # the job isn't run repeatedly.
                    if self.enabled:
                        self.logger.info("Manual run of %r", self.name)
                        scheduler.add_job(self.run, name=self.name)
                        self.__control.delete_job(self.id)

                else:

                    # An existing scheduled job has been modified.
                    self.logger.info("Modified %r registration", old.name)

                    # Swap the new `Control.Job` object into the dictionary.
                    self.__control.jobs[self.id] = self

                    # Tell the scheduler about the changes
                    opts = dict(func=self.run, name=self.name)
                    scheduler.modify_job(self.id, **opts)
                    if old.schedule != self.schedule:
                        opts = dict(self.schedule)
                        opts["trigger"] = "cron"
                        scheduler.reschedule_job(self.id, **opts)

                    # Handle a toggle of the `enabled` flag.
                    if not old.enabled and self.enabled:
                        scheduler.resume_job(self.id)
                    elif not self.enabled and old.enabled:
                        scheduler.pause_job(self.id)

            elif self.schedule:

                # A new job with a schedule: tell the scheduler about it
                # and plug it into the controller's `jobs` property.
                self.logger.info("Registered %r", self.name)
                opts = dict(self.schedule)
                opts["id"] = self.id
                opts["name"] = self.name
                if not self.enabled:
                    opts["next_run_time"] = None
                scheduler.add_job(self.run, "cron", **opts)
                self.__control.jobs[self.id] = self

            elif self.enabled:

                # When we find an enabled job with no schedule, we
                # run the job once and drop the row from the database.
                self.logger.info("Unscheduled run of %r", self.name)
                scheduler.add_job(self.run, name=self.name)
                self.__control.delete_job(self.id)

        def run(self):
            """Launch the job.

            This is the callback method invoked by the scheduler.
            Instantiate an instance of the job's implementation class
            and invoke its `run()` method.

            The job's implementing class is in a module of the `jobs`
            package. We use the standard library's `import_lib.import_module`
            to load the module (optimized by the interpreter for subsequent
            loads of the same module) and the builting `getattr()` on that
            module to get the class used to instantiate the implementing
            object for the job.
            """

            try:
                start = datetime.now()
                module_name, class_name = self.job_class.split(".", 1)
                module = import_module(f"jobs.{module_name}")
                args = self.__control, self.name
                getattr(module, class_name)(*args, **self.opts).run()
                args = start, datetime.now() - start, self.name
                self.logger.info("Job started %s, elapsed %s (%s)", *args)
            except Exception:
                self.logger.exception("%s started %s", self.name, start)


if __name__ == "__main__":
    """Don't execute the script if loaded as a module."""

    control = Control()
    try:
        control.run()
    except Exception as e:
        control.logger.exception("Failure")
        print(e)
