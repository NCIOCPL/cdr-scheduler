
import logging


from ndscheduler import job, utils, settings

logger = logging.getLogger(__name__)

class CDRJob(job.JobBase):

    def __init__(self, job_id, execution_id):
        job.JobBase.__init__(self, job_id, execution_id)
        

    @classmethod
    def meta_info(cls):
        return {
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            'notes': 'Infrastructure job type',
            'arguments': [
                # task name
                {'type': 'string', 'description': 'Class name of the specific task to run.'},
                # configuration document
                {'type': 'string', 'description': 'JSON-like configuration document (no apostrophes).'}
            ],
            'example_arguments': ''
        }

    def run(self, taskname, config, *args, **kwargs):
        raise NotImplementedError('CDRJob is not meant to be used directly.')

    def loadTask(self, taskname, config):
        logger.debug("Loading task: %s.%s", settings.TASK_CLASS_PACKAGE, taskname)
        taskclass = utils.import_from_path("%s.%s" % (settings.TASK_CLASS_PACKAGE, taskname))
        instance = taskclass(config, {})
        return instance

if __name__ == "__main__":
    job = CDRJob.create_test_instance()
    try:
        job.run({})
        print 'This was expected to fail but did not.'
    except NotImplementedError:
        print 'Failed in the expected manner.'
    pass
