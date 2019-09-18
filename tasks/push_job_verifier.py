"""Check the queue for push jobs awaiting verification.
"""

import datetime
import cdr
import cdrdb2 as cdrdb
import cdr2gk
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag


class Sweeper(CDRTask):
    """Implements subclass to check for push jobs awaiting verification.
    """

    LOGNAME = "push-job-verifier"
    MAX_HOURS = cdr.getControlValue("Publishing", "push-verify-wait") or 12

    def Perform(self):
        """Check any push jobs which are waiting to be verified.
        """

        self.conn = cdrdb.connect()
        self.cursor = self.conn.cursor()
        self.tier = cdr.Tier().name
        query = cdrdb.Query("pub_proc", "id", "completed")
        query.where("status = 'Verifying'")
        rows = query.order("id").execute(self.cursor).fetchall()
        for job_id, completed in rows:
            self.verify_load(job_id, completed)
        return TaskPropertyBag()

    def verify_load(self, job_id, completed):
        """Ask GateKeeper about the disposition of the job.
        """

        # Find out if the GateKeeper host was overridden from the default.
        query = cdrdb.Query("pub_proc_parm", "parm_value")
        query.where(query.Condition("pub_proc", job_id))
        query.where("parm_name = 'GKServer'")
        row = query.execute(self.cursor).fetchone()
        host = row[0] if (row and row[0]) else cdr2gk.HOST

        # Talk to the GateKeeper SOAP status service.
        args = job_id, host, completed
        self.logger.info("verifying push job %d to %s completed %s", *args)
        response = cdr2gk.requestStatus("Summary", job_id, host=host)
        details = response.details
        failures = []
        warnings = []
        verified = True
        for doc in details.docs:
            if doc.status == "Warning":
                warnings.append(doc)
            if "Error" in (doc.status, doc.dependentStatus):
                failures.append(doc)
            elif doc.location != "Live":
                verified = False
                break

        # If the load is finished, update the status of the job.
        if verified:
            self.logger.info("push completed with %d failures and %d warnings",
                             len(failures), len(warnings))

            # Mark failed docs.
            if failures:
                for doc in failures:
                    self.cursor.execute("""\
                        UPDATE pub_proc_doc
                           SET failure = 'Y'
                         WHERE pub_proc = %s
                           AND doc_id = %s""" % (job_id, doc.cdrId))
                self.conn.commit()

            # Notify the appropriate people of any problems found.
            if failures or warnings:
                self.report_problems(job_id, host, failures, warnings)

            # If every document failed the load, mark the status for the
            # entire job as Failure; however, if even 1 document was
            # successfully loaded to the live site, we must set the
            # status to Success; otherwise, software to find out whether
            # that document is on Cancer.gov may return the wrong answer.
            #
            # Note that if the attempt to report any problems fails,
            # we won't reach this code, because an exception will have
            # been thrown.  That's appropriate, because we don't want
            # to close out a job with problems going undetected.
            if len(failures) == len(details.docs):
                status = "Failure"
            else:
                status = "Success"
            self.cursor.execute("""\
                UPDATE pub_proc
                   SET status = '%s'
                 WHERE id = %d""" % (status, job_id))
            self.conn.commit()

        # The load hasn't yet finished; find out how long we've been waiting.
        # If it's been longer than MAX_HOURS hours, the job is probably stuck.
        # Note: This should only happen if very many summaries have to
        #       be processed.
        else:
            now = datetime.datetime.now()
            then = now - datetime.timedelta(hours=self.MAX_HOURS)
            if str(then) > str(completed):
                self.cursor.execute("""\
                    UPDATE pub_proc
                       SET status = 'Stalled'
                     WHERE id = %d""" % job_id)
                self.conn.commit()
                self.logger.error("job %d has stalled", job_id)
                self.report_problems(job_id, host, stalled=True)

    def report_problems(self, job_id, host, failures=None, warnings=None,
                        stalled=False):
        """Send out email notification of problems with a push job.
        """

        # Set the sender and recipients for the notification.
        sender = "cdr@%s" % cdr.CBIIT_NAMES[1]
        group = "Test PushVerificationAlerts"
        if self.tier == "PROD":
            group = "PushVerificationAlerts"
        recips = self.get_group_email_addresses(group)
        if not recips:
            group = "Test Publishing Notification"
            recips = self.get_group_email_addresses(group)
        if not recips:
            self.logger.error("no recipients for publishing notification")
            raise Exception("no recipients for publishing notification")

        # We've waited too long for the push job to finish.
        if stalled:
            subject = "[%s] Push job %d stalled" % (self.tier, job_id)
            body = """\
More than %d hours have elapsed since completion of the push of CDR
documents for publishing job %d, and loading of the documents
has still not completed.
""" % (self.MAX_HOURS, job_id)

        # The job finished, but there were problems reported.
        else:
            subject = ("[%s] Problems with loading of job %d "
                       "to GateKeeper" % (self.tier, job_id))
            body = """\
%d failures and %d warnings were encountered in the loading of documents
for job %d to GateKeeper.
""" % (len(failures), len(warnings), job_id)

        # Provide a link to a web page where the status of each document
        # in the job can be checked.
        url = ("%s/cgi-bin/cdr/GateKeeperStatus.py?"
               "jobId=%d&targetHost=%s&flavor=all" %
               (cdr.CBIIT_NAMES[2], job_id, host))

        body += """
Please visit the following link for further details:

%s
""" % url

        # Make sure the mail gets out.
        errors = cdr.sendMail(sender, recips, subject, body)
        if errors:
            self.logger.error("failure sending mail: %s" % errors)
            raise Exception("failure reporting problems(): %s" % errors)

