"""Sync SFTP logs with local file system

Also adds any new entries to the normalized cumulative log file.
"""

# Standard library modules
from datetime import date
from glob import glob
import gzip
from os import chdir

# Third-party modules
from dateutil.relativedelta import relativedelta

# Project modules
import cdr
from .base_job import Job


class Refresh(Job):
    """
    Job for refreshing local mirror of sftp pdq log files

    Attributes:
        logger - object for recording what we do
    """

    LOGNAME = "sftp-log-refresh"
    SYNCDIR = cdr.BASEDIR + "/sftp_log"
    SUPPORTED_PARAMETERS = {}

    def run(self):
        """
        Sync the log directory and top up the cumulative log file
        """

        self.logger.info("Refresh started")
        chdir(self.SYNCDIR)
        self.sync_logs()
        self.refresh_cumulative_log()
        self.logger.info("Refresh completed")

    @property
    def months(self):
        """Construct a map of month abbreviations to year, month tuples

        Once the old log data without the year field has flushed
        through the system, this can be simplified (and made more
        robust) by just mapping month abbreviations to month numbers,
        and use the years provided in the log entries instead of
        guessing.

        Return:
          dictionary with entries like "Jan" -> (2018, 1)
        """

        if not hasattr(self, "_months"):
            self._months = {}
            today = date.today()
            while len(self._months) < 12:
                name = today.strftime("%b")
                self._months[name] = today.year, today.month
                today -= relativedelta(months=1)
        return self._months

    def sync_logs(self):
        """
        Top up our local copies of the pdq logs from the sftp server.
        We're ignoring some expected errors, having to do with cygwin's
        difficulty in dealing with bizarre Windows file permissions
        configuration settings. If we really fail to bring down a needed
        log file successfully, we'll find out when we try to read it.
        """

        ssh = ("ssh -i d:/etc/cdroperator_rsa -o LogLevel=error "
               "-o StrictHostKeyChecking=no")
        usr = "cdroperator"
        dns = "cancerinfo.nci.nih.gov"
        src = "%s@%s:/sftp/sftphome/cdrstaging/logs/*" % (usr, dns)
        cmd = "rsync -e \"%s\" %s ." % (ssh, src)
        fix = r"%s:\cdr\bin\fix-permissions.cmd ." % cdr.WORK_DRIVE
        self.logger.info(cmd)
        cdr.run_command(cmd)
        self.logger.info(fix)
        cdr.run_command(fix)

    def normalize(self, line):
        """
        Transform log entry by converting sloppy date string to ISO format

        Pass:
          line - string for a line as it appeared in the original sftp log

        Return:
          string for transformed line with ISO date/time string
        """

        pieces = line.strip().split()
        if pieces[0].isdigit():
            pieces = pieces[1:]
        year, month = self.months[pieces[0]]
        day = int(pieces[1])
        date_string = "{:d}-{:02d}-{:02d} ".format(year, month, day)
        return date_string + " ".join(pieces[2:])

    def wanted(self, line):
        """
        Determine whether this is a line we want to keep

        We only include lines representing new sessions ("session opened
        for local user ...") or fetches of files (containing the substring
        "]: open").

        Pass:
          line - string for a line as it appeared in the original sftp log

        Return:
          True if we should copy line to cumulative.log; else False
        """

        return "session opened for local user" in line or "]: open " in line

    def refresh_cumulative_log(self):
        """
        Add new entries to cumulative.log file

        Make a note of the entries we have from the past three months,
        then fetch the lines from the latest monthly set of logs, as well
        as the current log, and append the ones we haven't seen yet to
        the cumulative.log file.
        """

        entries = set()
        today = date.today()
        cutoff = str(today - relativedelta(months=3))
        self.logger.info("using cutoff %s", cutoff)
        for line in open("cumulative.log"):
            if line > cutoff:
                entries.add(line.strip())
        self.logger.info("fetched %d existing entries", len(entries))
        monthlies = sorted(glob("pdq.log-????????.gz"))
        new_lines = []
        with gzip.open(monthlies[-1]) as fp:
            for line in fp.readlines():
                line = line.decode("utf-8")
                if self.wanted(line):
                    line = self.normalize(line)
                    if line not in entries:
                        entries.add(line)
                        new_lines.append(line)
        monthly_count = len(new_lines)
        if monthly_count:
            args = monthly_count, monthlies[-1]
            self.logger.info("saving %d lines from %s", *args)
        with open("pdq.log") as fp:
            for line in fp.readlines():
                if self.wanted(line):
                    line = self.normalize(line)
                    if line not in entries:
                        entries.add(line)
                        new_lines.append(line)
        current_count = len(new_lines) - monthly_count
        self.logger.info("saving %d lines from pdq.log", current_count)
        if new_lines:
            with open("cumulative.log", "ab") as fp:
                for line in new_lines:
                    fp.write(line.encode("utf-8"))
                    fp.write(b"\n")


if __name__ == "__main__":
    """
    Support command-line testing.
    """

    Refresh(None, "Refresh SFTP Logs").run()
