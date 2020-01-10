
from .cdr_task_base import CDRTask
from core.exceptions import TaskException

class ErrorTask(CDRTask):
    """
    Task that always raises an exception.
    """

    def __init__(self, jobParams, taskData):
        CDRTask.__init__(self, jobParams, taskData)

    def Perform(self):
        raise TaskException('This is an expected error.')

if __name__ == "__main__":
    # Test code
    task = ErrorTask({"a" : 1, "b" : 2, "c" : None}, {"data1" : "a", "data2" : "b"})
    try:
        task.Perform()
        print('This task was expected to fail but did not.')
    except TaskException:
        print('Failed in the expected manner.')
