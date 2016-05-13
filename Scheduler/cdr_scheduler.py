"""Run the scheduler process."""

from ndscheduler.server import server


class CDRScheduler(server.SchedulerServer):

    # Logic to cleanup outstanding jobs goes in start_scheduler().
    def start_scheduler(self):
        server.SchedulerServer.start_scheduler(self)



if __name__ == "__main__":
    CDRScheduler.run()
