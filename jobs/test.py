"""Stub job for testing basic operation of the scheduler.
"""

from .base_job import Job


class Stub(Job):
    def run(self):
        self.logger.info("Running %s job with opts %s", self.name, self.opts)


if __name__ == "__main__":
    Stub(None, "Scheduler Test", stooge="Larry", pep_boy="Manny").run()
