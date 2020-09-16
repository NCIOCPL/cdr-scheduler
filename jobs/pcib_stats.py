"""
Management report to list a variety of counts (typically run for the
previous month) regarding the number of documents published, updated, etc.
See https://tracker.nci.nih.gov/browse/OCECDR-3478 for original requirements
for this report.
"""

from .base_job import Job
from cdr_stats import Control


class ReportTask(Job):
    """
    Implements subclass for managing the monthly PCIB staticics report.
    """

    SUPPORTED_PARAMETERS = {
        "docs",
        "email",
        "end",
        "ids",
        "log-level",
        "max-docs",
        "mode",
        "recips",
        "sections",
        "start",
        "title",
    }
    def run(self):
        "Hand off the real work to the Control object."

        recips = self.opts.get("recips", "").split()
        if recips:
            self.opts["recips"] = recips
        control = Control(self.opts)
        control.run()


if __name__ == "__main__":
    """
    Make it possible to run this task from the command line.
    """

    cdr_stats.main()
