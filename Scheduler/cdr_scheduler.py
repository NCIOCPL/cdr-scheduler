"""Run the scheduler process."""

import settings
from ndscheduler.server import server


class CDRScheduler(server.SchedulerServer):

    # Logic to cleanup outstanding jobs goes in start_scheduler().
    def start_scheduler(self):
        self.fix_zombies()
        server.SchedulerServer.start_scheduler(self)

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
        settings.logging.info("found %d zombie executions", len(eids))
        for eid in eids:
            print eid, (repr(eid))
            settings.logging.info("marking zombie %s as FAILED", eid)
            datastore.update_execution(eid, state=failed, description=desc)


if __name__ == "__main__":
    CDRScheduler.run()
