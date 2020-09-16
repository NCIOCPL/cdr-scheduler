"""Launch publishing jobs awaiting processing.
"""

import os
import cdr
from cdrapi import db
from .base_job import Job


class Sweeper(Job):
    """Implements subclass to check for queued publishing jobs.
    """

    LOGNAME = "publish"
    PUBSCRIPT = cdr.BASEDIR + "/publishing/publish.py"
    SUPPORTED_PARAMETERS = {}

    def run(self):
        """Launch any publishing jobs which are in the queue.

        Make sure we don't do any real work if not on a Windows server.
        """

        conn = db.connect(user="CdrPublishing")
        cursor = conn.cursor()
        query = db.Query("pub_proc", "id", "pub_subset")
        query.where("status = 'Ready'")
        rows = query.execute(cursor).fetchall()
        if rows and os.name == "nt":
            cursor.execute("""\
                UPDATE pub_proc
                   SET status = 'Started'
                 WHERE status = 'Ready'""")
            conn.commit()
        for job_id, pub_subset in rows:
            self.logger.info("starting job %d (%s)", job_id, pub_subset)
            args = ("CdrPublish", self.PUBSCRIPT, str(job_id))
            if os.name == "nt":
                os.spawnv(os.P_NOWAIT, cdr.PYTHON, args)

if __name__ == "__main__":
    """Enable command-line testing."""
    Sweeper(None, "Publishing Queue").run()
