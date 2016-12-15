"""A sample job that prints string."""

import logging

from cdr_job_base import CDRJob
from core.const import TaskStatus
from core.exceptions import TaskException

logger = logging.getLogger(__name__)

class BasicJob(CDRJob):
    "Defines a CDRJob type which runs a single task."

    @classmethod
    def meta_info(cls):
        return {
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            'notes': 'Basic CDR Job',
            'arguments': [
                # task name
                {'type': 'string', 'description': 'Class name of the specific task to run.'},
                # configuration document
                {'type': 'string', 'description': 'JSON-like configuration document.'}
            ],
            'example_arguments': '["test_task.TestTask", {"property1" : 1, "property2" : "foo", "property3" : null}]'
        }

    def run(self, taskname, config, *args, **kwargs):
        task = self.loadTask(taskname, config)
        try:
            status = task.Perform()
            if status.GetStatus() != TaskStatus.OK:
                logger.error('Task %s returned status \'%d\'.', taskname, status.GetStatus())
        except TaskException as e:
            logger.exception(e)
            raise #re throw so this will be logged as an error.



if __name__ == "__main__":
    # You can easily test this job here
    job = BasicJob.create_test_instance()
    job.run("test_task.TestTask", {"a" : 1, "b" : 2, "c" : None}, {"data1" : "a", "data2" : "b"})
