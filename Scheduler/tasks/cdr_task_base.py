from core.exceptions import TaskException

class CDRTask(object):
    """
        Base class for all tasks performed by a CDRJob subclass.
    """

    def __init__(self, jobParams, taskData):
        """
        jobParams: configuration document passed to the parent job.
        taskData: arguments passed to the task which were calculated by the job
            (e.g. via another task).
        """
        if not(isinstance(jobParams, dict)):
            raise TypeError('jobParams must be a dictionary object. Got \'%s\' instead.' %
                            type(jobParams).__name__)
        if not(isinstance(taskData, dict)):
            raise TypeError('taskData must be a dictionary object. Got \'%s\' instead.' %
                            type(taskData).__name__)

        self.jobParams = jobParams
        self.taskData = taskData

    def Perform(self):
        """
        Performs the concrete task's work. Must be implmented in all CDRTask subclasses.
        """
        raise NotImplementedError('The Perform() method must be implemented in all CDRTask subclasses.')

    def get_required_param(self, name):
        if name in self.jobParams:
            return self.jobParams[name]
        else:
            raise TaskException("Required parameter '%s' not found." % (name,))

    def get_optional_param(self, name, default):
        if name in self.jobParams:
            return self.jobParams[name]
        else:
            return default
