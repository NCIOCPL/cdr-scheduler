"""Launch any batch jobs awaiting processing.
"""

import os
from .base_job import Job
import cdr
import cdrbatch
from cdrapi import db


class Sweeper(Job):

    LOGNAME = "batch-job-queue"

    def run(self):
        """Launch any batch jobs which are in the queue."""

        conn = db.connect(user="CdrPublishing")
        cursor = conn.cursor()
        query = db.Query("batch_job", "id", "command")
        query.where(query.Condition("status", cdrbatch.ST_QUEUED))
        for job in query.execute(cursor).fetchall():
            command = job.command
            if not os.path.isabs(command):
                command = f"{cdr.BASEDIR}/{command}"
            script = f"{command} {job.id}"
            if command.endswith(".py"):
                command = cdr.PYTHON
            else:
                command, script = script, ""
            args = conn, job.id, cdrbatch.ST_INITIATING, cdrbatch.PROC_DAEMON
            cdrbatch.sendSignal(*args)
            conn.commit()
            os.spawnv(os.P_NOWAIT, command, (command, script))
            self.logger.info("processed %s", command)
