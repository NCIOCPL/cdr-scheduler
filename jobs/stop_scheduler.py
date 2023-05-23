from .base_job import Job


class Stop(Job):
    SUPPORTED_PARAMETERS = {"dbserver"}

    def run(self):
        self.control.stop()
