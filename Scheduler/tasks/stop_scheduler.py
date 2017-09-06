"""Exit the current scheduler process, forcing a restart.
"""

import sys
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag

class Stop(CDRTask):
    """Implements subclass to stop the current CDR Scheduler instance.
    """

    LOGNAME = "scheduler-service"

    def Perform(self):
        """Exit so the service manager will launch a fresh process.
        """

        self.logger.info("exiting on request")
        sys.exit(1)
        return TaskPropertyBag()
