"""Check for problems with the production CDR server.
"""

from argparse import ArgumentParser
from datetime import datetime, timedelta
from functools import cached_property
from urllib.request import urlopen
from .base_job import Job
from cdr import EmailMessage


class Monitor(Job):

    LOGNAME = "health-check"
    URL = "https://cdr.cancer.gov/cgi-bin/cdr/cdrping.py"
    OK = "CDR OK"
    LAST_NOTIFICATION = None
    UNREPORTED_FAILURES = {}
    FROM = "NCIPDQoperator@mail.nih.gov"
    SUBJECT = "*** CDR PRODUCTION SERVER FAILURES ***"
    SUPPORTED_PARAMETERS = {"delay", "recips"}

    def run(self):
        self.check_health()
        self.notify()

    def check_health(self):
        """See if the production server is OK. If not, record the problem."""

        try:
            with urlopen(self.URL) as response:
                status = str(response.read(), encoding="utf-8").strip()
            if status != self.OK:
                self.logger.error(status)
                count = Monitor.UNREPORTED_FAILURES.get(status, 0)
                Monitor.UNREPORTED_FAILURES[status] = count + 1
        except Exception as e:
            self.logger.exception("Health check failure")
            key = str(e)
            count = Monitor.UNREPORTED_FAILURES.get(key, 0)
            Monitor.UNREPORTED_FAILURES[key] = count + 1

    def notify(self):
        """Send alert for unreported problems, if time to do so."""

        if Monitor.UNREPORTED_FAILURES and self.time_to_notify:
            problems = []
            for problem in Monitor.UNREPORTED_FAILURES:
                count = Monitor.UNREPORTED_FAILURES[problem]
                problems.append(f"{problem} ({count})")
            opts = dict(subject=self.SUBJECT, body="\n".join(problems))
            message = EmailMessage(self.FROM, self.recips, **opts)
            message.send()
            Monitor.LAST_NOTIFICATION = datetime.now()
            Monitor.UNREPORTED_FAILURES = {}

    @cached_property
    def delay(self):
        """How many minutes should we wait between notifications?"""

        delay = self.opts.get("delay")
        if delay:
            try:
                return timedelta(minutes=int(delay))
            except Exception:
                self.logger.exception("Invalid integer value for delay")
        return timedelta(minutes=60)

    @cached_property
    def recips(self):
        """List of email addresses to which alerts should be sent."""

        recips = self.opts.get("recips")
        if recips:
            if isinstance(recips, str):
                if "," in recips:
                    return [r.strip() for r in recips.split(",")]
                elif " " in recips:
                    return [r for r in recips.strip().split()]
                else:
                    return [recips.strip()]
        else:
            group = "Developers Notification"
            return Job.get_group_email_addresses(group)

    @cached_property
    def time_to_notify(self):
        """Has enough time elapsed since the last notification?"""

        if Monitor.LAST_NOTIFICATION is None:
            return True
        return datetime.now() >= Monitor.LAST_NOTIFICATION + self.delay



if __name__ == "__main__":
    """Execute script only if not loaded as a module."""

    parser = ArgumentParser()
    parser.add_argument("--recips")
    opts = vars(parser.parse_args())
    Monitor(None, "Health Check", **opts).run()
