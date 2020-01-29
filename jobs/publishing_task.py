"""
Logic for nightly and weekly CDR publishing jobs.
"""

import os
import cdr
from .base_job import Job


class PublishingTask(Job):
    """
    Implements subclass for kicking off scheduled CDR publishing jobs.

    Other publishing jobs are run directly from the CDR Admin interface.
    """

    LOGNAME = "publishing-task"

    def run(self):
        "Hand off the real work to the Control object."
        Control(self.opts, self.logger).run()


class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.
    """

    PUBPATH = "%s/Publishing" % cdr.BASEDIR
    "Location of the publishing scripts."

    EXTRA_ARGS = {
        "CG2Public.py": "--inputdir=Job%d",
    }
    "How we pass the job ID to some scripts (if we have one)."

    def __init__(self, options, logger):
        """
        Save the logger object and extract and validate the settings:

        schedule
            must be "weekly" or "nightly" (required)

        mode
            must be "test" or "live" (required)

        job-id
            if specified, must be an integer representing a publishing job ID

        skip-publish-step
            optional Boolean, defaults to False; if True, the first script
            is suppressed

        publish-only
            optional Boolean, defaults to False; if True, all but the first
            script are suppressed

        log-level
            "info", "debug", or "error"; defaults to "info"
        """

        self.logger = logger
        self.schedule = options.get("schedule")
        self.mode = options.get("mode")
        self.job_id = options.get("job-id")
        self.skip_publish_step = options.get("skip-publish-step", False)
        self.publish_only = options.get("publish-only", False)
        if self.schedule not in ("weekly", "nightly"):
            raise Exception("schedule must be 'weekly' or 'nightly'")
        if self.mode not in ("test", "live"):
            raise Exception("mode must be 'live' or 'test'")
        self.test = self.mode == "test"
        if self.job_id:
            try:
                self.job_id = int(self.job_id)
            except:
                raise Exception("job-id must be an integer if specified")

    def run(self):
        """Launch the following scripts synchronously.

        If any script fails, or email notifications fail, we throw an
        exception. Avoid the extra weekly steps if testing on a non-Windows
        system.

        SubmitPubJob.py
            Queues up the export and push requests and waits for them
            to finish. If publish-only option is True, this is the
            only script we launch. If the skip-publish-step is True,
            we skip this script.

        CG2Public.py
            Transforms documents which match the cancer.gov DTD into
            documents which conform to the DTD used by all of our other
            data partners. Full weekly job only.

        sftp-export-data.py
            Copies the documents and supporting files to a separate
            directory, performs additional processing - creating auxilliary
            files - and updates the FTP server using rsync.
            Full weekly job only.

        Notify_VOL.py
            Notifies the Visuals OnLine (VOL) team when a media document
            has been updated or added to Cancer.gov. Full weekly job only.

        CheckHotfixRemove.py
            Identifies any documents whose status has changed in such a
            way that they should be removed from cancer.gov with a manually-
            generated Hotfix-Remove request. Full weekly job only.

        """

        description = "%s%s" % (self.test and "test " or "", self.schedule)
        self.logger.info("%s publishing task started", description)
        self.notify("started")

        if not self.skip_publish_step:
            self.launch("SubmitPubJob.py", merge_output=True)
        if not self.publish_only and os.name == "nt":
            if self.schedule == "weekly":
                self.launch("CG2Public.py")
                self.launch("sftp-export-data.py", include_runmode=False,
                                                   include_pubmode=False)
                self.launch("Notify_VOL.py", include_pubmode=False)
                self.launch("CheckHotfixRemove.py", include_pubmode=False)
        self.notify("finished")
        self.logger.info("%s publishing task completed", description)

    def notify(self, stage):
        self.logger.info("sending %s notification", repr(stage))
        subject = "%s publishing %s" % (self.schedule, stage)
        message = "%s job %s successfully" % (self.schedule.title(), stage)
        self.send_mail(subject.title(), message)

    def quote_arg(self, arg):
        """
        Make sure the passed string is treated as a single argument
        by the shell's command processor.
        """

        return '"' + arg.replace('"', "'") + '"'

    def send_mail(self, subject, message):
        """
        Send email to the users who monitor publishing jobs.
        If the email sending command fails, log the problem.
        """

        path = "%s/PubEmail.py" % self.PUBPATH
        subject = self.quote_arg(subject)
        message = self.quote_arg(message)
        command = "%s %s %s %s" % (cdr.PYTHON, path, subject, message)
        process = cdr.run_command(command)
        if process.stderr:
            self.logger.error("sending email: %s", process.stderr)

    def report_error(self, script):
        """
        Log the fact that an error occurred (and where it happened).
        Send an email notification of the error as well, and then
        if this is the core script to create the published output,
        raise an exception so processing of the job will halt,
        because nothing beyond this point will have anything to do.
        """

        self.logger.error(script)
        subject = "*** Error in %s" % script
        message = "Program returned with error code. See log file."
        self.send_mail(subject, message)
        if script == "SubmitPubJob.py":
            raise Exception("failure in SubmitPubJob.py")

    def failed(self, script, result):
        """
        Check to see whether the launched job failed. Special logic
        for the first script we launch, which (if we're to believe
        the logic used by the old JobMaster script) could return
        a "success" code even if it failed.
        """

        if result.returncode:
            return True
        return script == "SubmitPubJob.py" and "Failure" in result.stdout

    def launch(self, script, include_pubmode=True, merge_output=False,
               include_runmode=True):
        """
        Execute the name Python script in a separate process and check
        to make sure it succeeded. If it didn't log and report the failure
        and throw an exception. All scripts take an argument indicating
        that it's running live or as a test. Most also take a second
        argument indicating whether this is a nightly publication job
        or the larger weekly job. For the few that don't take this second
        argument, pass include_pubmode=False. If the standard output and
        standard error output should be merged, pass merge_output=True.
        If a job ID is specified, some scripts take a third argument
        to pass that ID; the form of the argument is found in the EXTRA_ARG
        class property.
        """

        path = "%s/%s" % (self.PUBPATH, script)
        command = "python %s" % path
        if include_runmode:
            command += " --%s" % self.mode
        if include_pubmode:
            pubmode = (self.schedule == "weekly") and "export" or "interim"
            command += " --%s" % pubmode
        if self.job_id:
            pattern = self.EXTRA_ARGS.get(script)
            if pattern:
                command += " " + pattern % self.job_id
        self.logger.info(command)
        process = cdr.run_command(command, merge_output=merge_output)
        if self.failed(script, process):
            self.logger.debug(process.stdout)
            self.report_error(script)


if __name__ == "__main__":
    """
    Make it possible to run this task from the command line.
    """

    import argparse
    import logging
    formatter_class = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(description="Do some publishing",
                                     formatter_class=formatter_class)
    parser.add_argument("--schedule", choices=("nightly", "weekly"),
                        help="nightly (interim) or weekly (full) publishing",
                        required=True)
    parser.add_argument("--mode", choices=("test", "live"), required=True,
                        help="whether we should actually publish documents")
    parser.add_argument("--job-id", help="optional integer for publishing job",
                        type=int)
    parser.add_argument("--skip-publish-step", action="store_true",
                        help="do just the post-publishing steps")
    parser.add_argument("--publish-only", action="store_true",
                        help="skip the post-publishing steps")
    parser.add_argument("--log-level", choices=("info", "debug", "error"),
                        default="info", help="verbosity of scheduler logging")
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])

    logging.basicConfig(format=cdr.Logging.FORMAT,
                        level=args.log_level.upper())
    Control(opts, logging.getLogger()).run()
