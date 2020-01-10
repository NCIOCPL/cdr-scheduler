from .base_job import Job

class Stop(Job):
    def run(self):
        self.control.stop()

