"""
Task to bounce the CDR services (also able to bounce other services,
or just stop a service, as long as the account running the CDR
scheduler has the right permissions).

OCECDR-4266: remove support for retired publishing service
"""

import time
from core.exceptions import TaskException
from task_property_bag import TaskPropertyBag
from cdr_task_base import CDRTask
import cdr

class BounceTask(CDRTask):
    """
    Implements subclass for managing the weekly bounce of the CDR services
    """

    def __init__(self, parms, data):
        """
        Initialize the base class then instantiate our Control object,
        which does all the real work. The data argument is ignored.
        """

        CDRTask.__init__(self, parms, data)
        self.control = Control(parms)

    def Perform(self):
        "Hand off the real work to the Control object."
        self.control.run()
        return TaskPropertyBag()


class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.

    Class constants:

    CDR_SERVICE   Display name for the CDR service
    """

    CDR_SERVICE = "Cdr"

    def __init__(self, options):
        """
        Save a reference to the options and create a logging object.
        """

        self.options = options
        self.logger = cdr.Logging.get_logger("cdr-service-control")

    def run(self):
        """
        Make sure at most one custom action is requested. If it is,
        perform it. Otherwise, perform the default behavior of the
        following steps:

            1. Stop the CDR service
            2. Start the CDR service
        """

        # Make sure at most one custom action is requested.
        bounce = self.options.get("bounce")
        stop = self.options.get("stop")
        if bounce and stop:
            problem = "bounce and stop options are mutually exclusive"
            self.logger.error(problem)
            raise TaskException(problem)

        # Bounce service if requested.
        if bounce:
            self.stop_service(bounce)
            self.start_service(bounce)
            return

        # Stop service if requested.
        if stop:
            self.stop_service(stop)
            return

        # Default behavior.
        self.stop_service(self.CDR_SERVICE)
        self.start_service(self.CDR_SERVICE)

    def stop_service(self, name):
        """
        Stop a service. User has to have admin rights on the machine.
        Don't do anything if the service is already stopped (except
        log the fact).

        name    Name of the service as shown in "net start ..."
        """

        # See if the service is already stopped.
        if not Control.service_is_running(name):
            self.logger.info("stop_service(%s): service not running", name)
            return

        # Use Microsoft's command-line utility to stop the service.
        result = cdr.runCommand("net stop \"%s\"" % name)
        if result.code:
            problem = ("stop_service(%s): error code %s (%s)" %
                       (name, result.code, result.output))
            self.logger.error(problem)
            raise TaskException(problem)

        # Wait for the dust to settle.
        time.sleep(5)

        # Make sure the service is stopped.
        if self.service_is_running(name):
            problem = "stop_service(%s): service still running" % name
            self.logger.error(problem)
            raise TaskException(problem)

        # Looks like we succeeded.
        self.logger.info("service %s successfully stopped", name)

    def start_service(self, name):
        """
        Start a service. User has to have admin rights on the machine.
        Don't do anything if the service is already started (except
        log the fact).

        name    Name of the service as shown in "net start ..."
        """

        # See if the service is already started.
        if Control.service_is_running(name):
            self.logger.info("start_service(%s): service already running", name)
            return

        # Use Microsoft's command-line utility to start the service.
        result = cdr.runCommand("net start \"%s\"" % name)
        if result.code:
            problem = ("start_service(%s): error code %s (%s)" %
                       (name, result.code, result.output))
            self.logger.error(problem)
            raise TaskException(problem)

        # Wait for the dust to settle.
        time.sleep(5)

        # Make sure the service is started.
        if not self.service_is_running(name):
            problem = "start_service(%s): service still not running" % name
            self.logger.error(problem)
            raise TaskException(problem)

        # Looks like we succeeded.
        self.logger.info("service %s successfully started", name)

    @staticmethod
    def service_is_running(name):
        """
        Use Microsoft's command-line utility to find out if a particular
        service is already running.

        name    display name for the service
        """

        command = "net start | grep -qi \"^ *%s$\"" % name
        return not cdr.runCommand(command).code

def main():
    """
    Make it possible to run this as a script from the command line.
    """

    import argparse
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--bounce", metavar="SERVICE")
    group.add_argument("--stop", metavar="SERVICE")
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    Control(opts).run()

if __name__ == "__main__":
    """
    Run the job if loaded as a script (not a module).
    """

    main()
