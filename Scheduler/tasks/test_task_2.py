
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag

class TestTask2(CDRTask):
    """
    Sample Task that outputs the contents of the jobParams and taskData
    constructor arguments.
    """

    def __init__(self, jobParams, taskData):
        CDRTask.__init__(self, jobParams, taskData)

    def Perform(self):
        result = TaskPropertyBag()

        print 'The Other Test Task!'
        print 'jobParams:'
        for item in self.jobParams:
            print "\t'%s' = '%s'" % (item, self.jobParams[item])

        print ' taskData:'
        for item in self.taskData:
            print "\t'%s' = '%s'" % (item, self.taskData[item])

        return result

if __name__ == "__main__":
    # Test code
    task = TestTask({"a" : 1, "b" : 2, "c" : None}, {"data1" : "a", "data2" : "b"})
    task.Perform()
