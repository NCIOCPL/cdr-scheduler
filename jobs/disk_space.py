"""Monitor free disk space.

Scheduled to run on DEV, reporting on all four tiers, so it's going into
svn trunk, even though it won't be installed on the upper tiers until the
next major release.

Uses output from the df.py CGI script, which comes back in a format like this:

C DRIVE
  TOTAL:  148202582016 (138.0G)
   USED:  119805886464 (111.6G)
   FREE:   28396695552 (26.4G)

D DRIVE
  TOTAL: 1281052635136 (1.2T)
   USED: 1013544005632 (943.9G)
   FREE:  267508629504 (249.1G)
"""

import argparse
import re
import requests
import cdr
from .base_job import Job


class Monitor(Job):
    """Send out alerts when available disk space is too low.
    """

    TIERS = "PROD", "STAGE", "QA", "DEV"
    LOGNAME = "disk-space-monitor"
    GB = 1024 * 1024 * 1024
    FROM = "NCIPDQoperator@mail.nih.gov"

    @property
    def recips(self):
        if not hasattr(self, "_recips"):
            recips = self.opts.get("recips")
            if recips:
                if isinstance(recips, str):
                    if "," in recips:
                        recips = [r.strip() for r in recips.split(",")]
                    else:
                        recips = [recips.strip()]
            else:
                group = "Developers Notification"
                recips = Job.get_group_email_addresses(group)
            self._recips = recips
        return self._recips

    @property
    def thresholds(self):
        if not hasattr(self, "_thresholds"):
            self._thresholds = {
                "C": int(self.opts.get("cthreshold") or 10),
                "D": int(self.opts.get("dthreshold") or 100),
            }
        return self._thresholds

    def run(self):
        self.logger.info("started")
        try:
            self.check()
        except Exception as e:
            self.logger.exception("failure")
            opts = dict(subject="DISK CHECK FAILURE", body=str(e))
            message = cdr.EmailMessage(self.FROM, self.recips, **opts)
            message.send()

    def check(self):
        problems = []
        for tier in self.TIERS:
            try:
                server = self.Server(tier)
            except Exception as e:
                self.logger.exception("failure checking %s", tier)
                problem = f"failure checking {tier}: {e}"
                problems.append(problem)
                continue
            for drive in sorted(server.free):
                free = server.free[drive]
                if free.bytes < self.thresholds[drive] * self.GB:
                    amount = f"{free.bytes} bytes {free.human}"
                    args = drive, server.name, amount
                    problem = "{}: drive on {} server down to {}".format(*args)
                    self.logger.warning(problem)
                    problems.append(problem)
        if problems:
            subject = "*** WARNING: CDR DISK SPACE CHECK ***"
            opts = dict(subject=subject, body="\n".join(problems))
            message = cdr.EmailMessage(self.FROM, self.recips, **opts)
            message.send()
            self.logger.info("sent alert to %s", ", ".join(self.recips))
        else:
            self.logger.info("disk space OK")

    class Server:
        BASE = "https://cdr{}.cancer.gov/cgi-bin/cdr/df.py"
        def __init__(self, tier):
            self.name = tier
            self.free = {}
            suffix = ""
            if self.name != "PROD":
                suffix = "-%s" % tier.lower()
            url = self.BASE.format(suffix)
            drive = None
            for line in requests.get(url).text.splitlines():
                if "DRIVE" in line:
                    drive = line[0]
                elif "FREE" in line:
                    self.free[drive] = self.Free(line)
        class Free:
            def __init__(self, line):
                tokens = line.strip().split()
                self.bytes = int(tokens[1])
                self.human = tokens[2]

    @classmethod
    def test(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("--cthreshold", default=25, type=int)
        parser.add_argument("--dthreshold", default=50, type=int)
        parser.add_argument("--recips")
        parser.add_argument("--level", default="info")
        opts = vars(parser.parse_args())
        opts["log-level"] = opts.get("level")
        Monitor(None, "Disk Space Test", **opts).run()


if __name__ == "__main__":
    Monitor.test()
