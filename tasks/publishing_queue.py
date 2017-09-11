"""Launch publishing jobs awaiting processing.
"""

import os
import cdr
import cdrdb2 as cdrdb
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag


class Sweeper(CDRTask):
    """Implements subclass to check for queued publishing jobs.
    """

    LOGNAME = "publish"
    PUBSCRIPT = cdr.BASEDIR + "/publishing/publish.py"

    def Perform(self):
        """Launch any publishing jobs which are in the queue.
        """

        conn = cdrdb.connect("CdrPublishing")
        cursor = conn.cursor()
        query = cdrdb.Query("pub_proc", "id", "pub_subset")
        query.where("status = 'Ready'")
        rows = query.execute(cursor).fetchall()
        if rows:
            cursor.execute("""\
                UPDATE pub_proc
                   SET status = 'Started'
                 WHERE status = 'Ready'""")
            conn.commit()
        for job_id, pub_subset in rows:
            self.logger.info("starting job %d (%s)", job_id, pub_subset)
            args = ("CdrPublish", self.PUBSCRIPT, str(job_id))
            os.spawnv(os.P_NOWAIT, cdr.PYTHON, args)
        return TaskPropertyBag()
