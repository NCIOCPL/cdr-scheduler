"""
Launch scripts for fetching and importing clinical trial documents
from CTRP.
"""

import logging
import cdr
from .cdr_task_base import CDRTask
from .task_property_bag import TaskPropertyBag

class ClinicalTrialsTask(CDRTask):
    """
    Implements subclass for managing scheduled download and import
    of clinical_trial documents from CTRP.
    """

    def __init__(self, parms, data):
        """
        Initialize the base class then instantiate our Control object,
        which does all the real work. The parms and data arguments
        are ignored.
        """

        CDRTask.__init__(self, parms, data)
        self.control = Control()

    def Perform(self):
        "Hand off the real work to the Control object."
        self.control.run()
        return TaskPropertyBag()

class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.
    """

    UTILPATH = "%s/Utilities" % cdr.BASEDIR
    "Location of the download and import scripts."

    def __init__(self):
        "Set up logging. No options to worry about."
        self.logger = logging.getLogger("clinical_trials_task")
        self.logger.setLevel(logging.INFO)

    def run(self):
        """
        Launch the two scripts. Even if the first one fails (for
        example, CTRP didn't give us a new trial set), we still
        execute the second one.
        """

        self.launch("DownloadCTGovProtocols.py")
        self.launch("ImportCTGovProtocols.py")

    def launch(self, script):
        """
        Launch the named script and log whether it succeeded or failed.
        Each script will take care of detailed logging of its own
        processing, as well as email notification of the appropriate
        staff of the results.
        """

        command = "%s/%s" % (self.UTILPATH, script)
        self.logger.info("%s started", command)
        result = cdr.runCommand(command)
        if result.code:
            self.logger.error("%s - failure code %s", command, result.code)
        else:
            self.logger.info("%s completed", command)

if __name__ == "__main__":
    """
    Make it possible to run this task from the command line.
    """

    logging.basicConfig()
    Control().run()
