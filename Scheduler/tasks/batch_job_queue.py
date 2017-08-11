"""Launch any batch jobs awaiting processing.
"""

import os
import cdr
import cdrbatch
import cdrdb2 as cdrdb
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag


class Sweeper(CDRTask):
    """Implements subclass to check for queued batch jobs.
    """

    LOGNAME = "batch-job-queue"

    def Perform(self):
        """Launch any batch jobs which are in the queue.
        """

        conn = cdrdb.connect("CdrPublishing")
        cursor = conn.cursor()
        query = cdrdb.Query("batch_job", "id", "command")
        query.where(query.Condition("status", cdrbatch.ST_QUEUED))
        jobs = query.execute(cursor).fetchall()
        for job_id, command in jobs:
            self.logger.info("job %d (%r)", job_id, command)
            if not os.path.isabs(command):
                command = cdr.BASEDIR + "/" + command
            script = "%s %d" % (command, job_id)
            if command.endswith(".py"):
                command = cdr.PYTHON
            else:
                command, script = script, ""
            cdrbatch.sendSignal(conn, job_id, cdrbatch.ST_INITIATING,
                                cdrbatch.PROC_DAEMON)
            conn.commit()
            os.spawnv(os.P_NOWAIT, command, (command, script))
            self.logger.info("found job %d (%s)", job_id, command)
        return TaskPropertyBag()
