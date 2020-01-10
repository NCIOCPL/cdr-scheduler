"""
Management report to list a variety of counts (typically run for the
previous month) regarding the number of documents published, updated, etc.
See https://tracker.nci.nih.gov/browse/OCECDR-3478 for original requirements
for this report.
"""

from .cdr_task_base import CDRTask
from core.exceptions import TaskException
from .task_property_bag import TaskPropertyBag
import cdr_stats


class ReportTask(CDRTask):
    """
    Implements subclass for managing the monthly PCIB staticics report.
    """

    def __init__(self, parms, data):
        """
        Initialize the base class then instantiate our Control object,
        which does all the real work. The data argument is ignored.
        """

        CDRTask.__init__(self, parms, data)
        self.control = cdr_stats.Control(parms)

    def Perform(self):
        "Hand off the real work to the Control object."
        self.control.run()
        return TaskPropertyBag()



if __name__ == "__main__":
    """
    Make it possible to run this task from the command line.
    You'll have to modify the PYTHONPATH environment variable
    to include the parent of this file's directory.
    """

    cdr_stats.main()
