"""Show scheduled jobs queued to be run.
"""

from .base_job import Job
from cdr import EmailMessage, OPERATOR
from cdrapi.settings import Tier
from io import StringIO


class Reporter(Job):

    SUPPORTED_PARAMETERS = {"recips"}

    def run(self):
        opts = dict(subject=self.subject, body=self.report)
        EmailMessage(OPERATOR, self.recips, **opts).send()
        self.logger.info("Sent jobs list to %s", self.recips)

    @property
    def report(self):
        """Body for the report's email message."""

        if not hasattr(self, "_report"):
            with StringIO() as stream:
                self.control.scheduler.print_jobs(out=stream)
                self._report = stream.getvalue()
        return self._report

    @property
    def recips(self):
        """Where to send the report."""

        if not hasattr(self, "_recips"):
            recips = self.opts.get("recips")
            if recips:
                self._recips = [r.strip() for r in recips.split(",")]
            else:
                self._recips = self.get_group_email_addresses()
        return self._recips

    @property
    def subject(self):
        """Subject line for the report's email message."""

        if not hasattr(self, "_subject"):
            tier = Tier().name
            self._subject = f"[{tier}] CDR Pending Scheduled Jobs"
        return self._subject
