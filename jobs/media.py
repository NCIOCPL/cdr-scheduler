"""Support for media documents stored in the cloud using Akamai.

Compares the media files in the Akamai rsync directory with those
generated from the blobs last published for the CDR Media documents.
If there are any discrepancies or errors, an email message is sent
to the developers.

None of the following parameter options are required:

  fix
    set to any non-empty string to cause the discrepancies to be
    corrected, so that the file system has the media files it should

  nolock
    set to any non-empty string to prevent the media directory from
    being renamed to media.lock during processing so that no other
    job can run which modifies the media files (should probably not
    be used except during develop testing, as this option could
    introduce unpredictable behavior, or leave the file system in
    an incorrect state); ignored if the `fix` option is set

  recip
    overrides the default recipient list for any email notification
    sent out; multiple recipients can be specified, separated by
    commas and/or spaces

  rsync
    set to any non-empty string to cause `rsync` to be run to give
    any changes to Akamai

  force
    set to any non-empty string to cause `rsync` to be run even if
    there are no discrepancies detected between the file system and
    the repository

  debug
    set to any non-empty string to cause each Media document checked
    to be logged
"""

from .base_job import Job
from cdr import EmailMessage
from cdrpub import Control
from cdrapi import db
from cdrapi.docs import Doc
from cdrapi.users import Session
from datetime import date, datetime
from glob import glob
from os import chdir
from re import search
from shutil import rmtree


class Check(Job):
    """Report on discrepancies between repository and file system."""

    LOGNAME = "check-media"
    PATTERNS = f"audio/*.mp3", f"images/*.jpg"

    def run(self):
        start = datetime.now()
        if self.debug:
            self.logger.setLevel("DEBUG")
        self.errors = []
        self.report_rows = []
        try:
            self.compare()
            if self.rsync:
                args = self.session.tier.name, self.logger, self.directory
                self.logger.info("Running rsync from %s", self.directory)
                Control.Media.rsync(*args)
            chdir(Control.Media.AKAMAI)
            if self.fix:
                if self.report_rows:
                    Control.Media.promote(self.directory)
                else:
                    rmtree(self.directory)
                    Control.Media.unlock()
            elif self.lock:
                Control.Media.unlock()
        except Exception as e:
            self.logger.exception("media check failure")
            self.errors.append(str(e))
            self.report_rows = []
            if self.fix or self.lock:
                try:
                    Control.Media.unlock()
                except Exception as e:
                    self.logger.exception("Unlock failure")
                    self.errors.append(str(e))
        self.logger.info("Elapsed: %s", datetime.now() - start)
        self.send_report()

    def compare(self):
        """Find out which files need to be added/changed/removed."""

        ids = set()
        for id, version in self.docs:
            self.logger.debug("Checking CDR%d version %d", id, version)
            ids.add(id)
            doc = Doc(self.session, id=id, version=version)
            files = Control.Media.get_files(doc)
            disk_files = self.catalog.get(id, set())
            paths = set()
            for f in files:
                paths.add(f.path)
                if f.path in disk_files:
                    with open(f.path, "rb") as fp:
                        file_bytes = fp.read()
                    if file_bytes != f.bytes:
                        self.report_rows.append((f.path, "modified"))
                        self.logger.info("%s changed", f.path)
                        if self.fix:
                            with open(f.path, "wb") as fp:
                                fp.write(f.bytes)
                else:
                    self.report_rows.append((f.path, "added"))
                    self.logger.info("%s added", f.path)
                    if self.fix:
                        with open(f.path, "wb") as fp:
                            fp.write(f.bytes)
            for p in disk_files - paths:
                self.report_rows.append((p, "dropped"))
                self.logger.info("%s dropped")
                if self.fix:
                    os.remove(p)
        for id in self.catalog:
            if id not in ids:
                self.logger.info("CDR%d no longer published", id)
                for p in self.catalog[id]:
                    self.report_rows.append((p, "dropped"))
                    if self.fix:
                        os.remove(p)

    def send_report(self):
        """Send email message listing media file changes."""

        if self.errors or self.report_rows or self.rsync:
            subject = f"[{self.session.tier}] Media File Check"
            opts = dict(subject=subject, body=self.report, subtype="html")
            message = EmailMessage(self.SENDER, self.recips, **opts)
            message.send()
            self.logger.info("Notified %s", ", ".join(self.recips))
        else:
            self.logger.info("No mismatches or errors to report")

    @property
    def catalog(self):
        """Media files in the current directory."""

        if not hasattr(self, "_catalog"):
            chdir(self.directory)
            self.logger.info("Cataloging files in %s", self.directory)
            self._catalog = {}
            for pattern in self.PATTERNS:
                suffix = pattern[-3:]
                target = fr"(\d+)(-\d+)?\.{suffix}"
                for path in glob(pattern):
                    match = search(target, path)
                    id = int(match.group(1))
                    path = path.replace("\\", "/")
                    if id not in self._catalog:
                        self._catalog[id] = {path}
                    else:
                        self._catalog[id].add(path)
            self.logger.info("Cataloged files for %d docs", len(self._catalog))
        return self._catalog

    @property
    def debug(self):
        """If True, crank up logging level."""
        return True if self.opts.get("debug") else False

    @property
    def directory(self):
        """Path to files we will compare."""

        if not hasattr(self, "_directory"):
            self._directory = Control.Media.MEDIA
            if self.lock:
                Control.Media.lock()
                self._directory = Control.Media.LOCK
            if self.fix:
                self._directory = Control.Media.clone()
        return self._directory

    @property
    def docs(self):
        """Sequence of id/version tuples for the published media documents."""

        if not hasattr(self, "_docs"):
            query = db.Query("all_docs a", "c.id", "d.doc_version").unique()
            query.join("pub_proc_cg c", "c.id = a.id")
            query.join("doc_type t", "t.id = a.doc_type")
            query.join("pub_proc_doc d", "d.doc_id = c.id",
                       "d.pub_proc = c.pub_proc")
            query.where("t.name = 'Media'")
            rows = query.execute().fetchall()
            self._docs = sorted([tuple(row) for row in rows])
            self.logger.info("Repository has %d media docs", len(rows))
        return self._docs

    @property
    def fix(self):
        """Should we correct the mismatches? (Default is False)."""

        if not hasattr(self, "_fix"):
            self._fix = True if self.opts.get("fix") else False
        return self._fix

    @property
    def lock(self):
        """Should we lock the media files (default is True)?"""

        if not hasattr(self, "_lock"):
            if self.fix:
                self._lock = True
            else:
                self._lock = False if self.opts.get("nolock") else True
        return self._lock

    @property
    def recips(self):
        """Who should get the report."""

        if not hasattr(self, "_recips"):
            recip = self.opts.get("recip", "").strip().replace(",", " ")
            if recip:
                self._recips = recip.split()
            else:
                group = "Developers Notification"
                self._recips = self.get_group_email_addresses(group)
        return self._recips

    @property
    def report(self):
        """HTML body for the email report."""

        if not hasattr(self, "_report"):
            title = "Media Files Check"
            style = "font-size: .9em; font-style: italic; font-family: Arial"
            today = date.today()
            body = self.B.BODY(
                self.B.H3(title, style="color: navy; font-family: Arial;"),
                self.B.P("Report date: {}".format(today), style=style)
            )
            style = "font-weight: bold; color: red; font-family: Arial"
            for error in self.errors:
                body.append(self.B.P(error, style=style))
            if not self.errors and self.rsync:
                message = f"Ran rsync from {self.directory} to Akamai."
                style = "font-weight: bold; color: blue; font-family: Arial"
                body.append(self.B.P(message, style=style))
            if self.report_rows:
                body.append(self.table)
            head = self.B.HEAD(
                self.B.META(charset=self.CHARSET),
                self.B.TITLE("Media Files Check")
            )
            report = self.B.HTML(head, body)
            self._report = self.serialize(report)
        return self._report

    @property
    def table(self):
        """Deltas found (and possibly corrected)."""

        if not hasattr(self, "_table"):
            style = "font-weight: bold; font-size: 1.2em; font-family: Arial"
            style += "; text-align: left;"
            self._table = self.B.TABLE(
                self.B.CAPTION("Changes in Media Files", style=style),
                self.B.TR(
                    self.th("Path"),
                    self.th("Action")
                ),
                style=self.TSTYLE
            )
            for path, action in self.report_rows:
                tr = self.B.TR(
                    self.td(path),
                    self.td(action)
                )
                self._table.append(tr)
        return self._table

    @property
    def rsync(self):
        """If True, give Akamai the changes."""

        if not hasattr(self, "_rsync"):
            if self.opts.get("force"):
                self._rsync = True
            elif not self.report_rows:
                self._rsync = False
            elif self.opts.get("rsync"):
                if not self.fix:
                    message = "Ignoring rsync request for mismatched files"
                    self.logger.warning(message)
                    self.errors.append(message)
                    self._rsync = False
                else:
                    self._rsync = True
            else:
                self._rsync = False
        return self._rsync

    @property
    def session(self):
        """For fetching blobs from media documents."""

        if not hasattr(self, "_session"):
            self._session = Session("guest")
        return self._session


if __name__ == "__main__":
    """Support command-line testing."""

    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--recip")
    parser.add_argument("--fix", action="store_true")
    parser.add_argument("--rsync", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--nolock", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    opts = dict([(k, v) for k, v in args._get_kwargs()])
    Check(None, "Test Media Files Check", **opts).run()
