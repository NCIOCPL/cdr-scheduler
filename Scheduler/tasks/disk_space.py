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
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag

class Monitor(CDRTask):
    """Send out alerts when available disk space is too low.
    """

    TIERS = "PROD", "STAGE", "QA", "DEV"
    LOGNAME = "disk-space-monitor"
    GB = 1024 * 1024 * 1024
    FROM = "NCIPDQoperator@mail.nih.gov"

    def __init__(self, parms, data):
        CDRTask.__init__(self, parms, data)
        self.logger.info("started")
        self.thresholds = {
            "C": parms.get("cthreshold") or 10,
            "D": parms.get("dthreshold") or 25
        }
        recips = parms.get("recips")
        self.recips = []
        if recips:
            self.recips = [r.strip() for r in recips.split(",")]
        if not self.recips:
            group = "Developers Notification"
            self.recips = CDRTask.get_group_email_addresses(group)

    def Perform(self):
        try:
            self.check()
        except Exception as e:
            self.logger.exception("failure")
            cdr.sendMail(self.FROM, self.recips, "DISK CHECK FAILURE", str(e))
        finally:
            return TaskPropertyBag()
    def check(self):
        problems = []
        for tier in self.TIERS:
            server = self.Server(tier)
            for drive in sorted(server.free):
                free = server.free[drive]
                if free.bytes < self.thresholds[drive] * self.GB:
                    amount = "%d bytes %s" % (free.bytes, free.human)
                    args = drive, server.name, amount
                    problem = "%s: drive on %s server down to %s" % args
                    self.logger.warning(problem)
                    problems.append(problem)
        if problems:
            subject = "WARNING: LOW CDR DISK SPACE"
            cdr.sendMail(self.FROM, self.recips, subject, "\n".join(problems))
            self.logger.info("sent alert to %s", ", ".join(self.recips))
        else:
            self.logger.info("disk space OK")

    class Server:
        BASE = "https://cdr%s.cancer.gov/cgi-bin/cdr/df.py"
        def __init__(self, tier):
            self.name = tier
            self.free = {}
            suffix = ""
            if self.name != "PROD":
                suffix = "-%s" % tier.lower()
            url = self.BASE % suffix
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
        Monitor(opts, {}).Perform()
if __name__ == "__main__":
    Monitor.test()
