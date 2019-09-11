
import logging


from ndscheduler import job, utils, settings

logger = logging.getLogger(__name__)

# Abstract base class for all jobs in the CDR Scheduling system.
class CDRJob(job.JobBase):

    def __init__(self, job_id, execution_id):
        job.JobBase.__init__(self, job_id, execution_id)


    @classmethod
    # Provides information for displaying in the scheduler's user interface.
    def meta_info(cls):
        return {
            # Copy job_class_string as-is. This will display the class name.
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            # Notes: Describe what the job does.
            'notes': 'Infrastructure job type',
            # Descriptions of the job's arguments.  These values are displayed exactly to the user.
            #   type: The python type that is expected.
            #   descrption: A description of the argument.
            'arguments': [
                # task name
                {'type': 'string', 'description': 'Class name of the specific task to run.'},
                # configuration document
                {'type': 'string', 'description': 'JSON-like configuration document (no apostrophes).'}
            ],
            # example_arguments: An example of what the argument list might look like.
            'example_arguments': ''
        }

    # All concrete job classes must implement run(). After self, supply any named arguments that user will be
    # providing. The array of arguments in the user interface will be passed as the matching named parameter in
    # the method's argument list.
    def run(self, *args, **kwargs):
        raise NotImplementedError('CDRJob is not meant to be used directly.')

    # Utility method for loading a task class from a .PY file and instantiating it.  All job classes should use
    # loadTask to create their task instances.
    def loadTask(self, taskname, config):
        logger.debug("Loading task: %s.%s", settings.TASK_CLASS_PACKAGE, taskname)
        taskclass = utils.import_from_path("%s.%s" % (settings.TASK_CLASS_PACKAGE, taskname))
        instance = taskclass(config, {})
        return instance

if __name__ == "__main__":
    job = CDRJob.create_test_instance()
    try:
        job.run({})
        print('This was expected to fail but did not.')
    except NotImplementedError:
        print('Failed in the expected manner.')
    pass
