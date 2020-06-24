"""Stub job for testing basic operation of the scheduler.
"""

from .base_job import Job
from cdr import run_command


class Stub(Job):
    def run(self):
        self.logger.info("Running %s job with opts %s", self.name, self.opts)
        if "command" in self.opts:
            process = run_command(self.opts["command"])
            self.logger.info("returncode: %d", process.returncode)
            if process.stdout:
                self.logger.info("stdout: %s", process.stdout)
            if process.stderr:
                self.logger.info("stderr: %s", process.stderr)


if __name__ == "__main__":
    Stub(None, "Scheduler Test", stooge="Larry", pep_boy="Manny").run()
