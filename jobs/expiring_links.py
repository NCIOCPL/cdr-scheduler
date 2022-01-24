"""Email monthly report of links about to be deleted.
"""

from argparse import ArgumentParser
from functools import cached_property
from dateutil import relativedelta, utils
from lxml import html
from lxml.html import builder
from .base_job import Job
from cdr import EmailMessage, Tier
from cdrapi import db


class ReportTask(Job):
    """
    Implements subclass for managing the scheduled report.
    """

    LOGNAME = "expiring_links"
    SUPPORTED_PARAMETERS = {
        "mode",
        "recip",
        "cutoff",
    }
    SENDER = "PDQ Operator <NCIPDQoperator@mail.nih.gov>"
    DRUG_REFERENCE = "/DrugInformationSummary/DrugReference"
    U_PATH = f"{DRUG_REFERENCE}/DrugReferenceLink/@cdr:xref"
    D_PATH = f"{DRUG_REFERENCE}/DrugReferencePostedDate"
    T_PATH = f"{DRUG_REFERENCE}/@TemporaryLink"
    STYLE = {
        "font-family: Arial, sans-serif",
        "width: 80%",
        "border: 1px solid #999",
        "border-collapse: collapse",
        "margin-top: 30px",
    }
    T_STYLE = "; ".join(STYLE)
    C_STYLE = "font-size: 1.5em; font-weight: bold;"

    def run(self):
        """Find the old links and report them."""
        self.report.send()

    @cached_property
    def cursor(self):
        """Access to the database."""
        return db.connect(user="CdrGuest").cursor()

    @cached_property
    def cutoff(self):
        """Date after which a temporary link goes stale."""

        cutoff = self.opts.get("cutoff")
        if cutoff:
            return cutoff
        now = utils.today()
        then = now - relativedelta.relativedelta(years=+3, months=-1)
        return then.strftime("%Y-%m-%d")

    @cached_property
    def links(self):
        fields = "u.doc_id", "u.value AS url", "d.value AS date"
        query = db.Query("query_term u", *fields).unique()
        query.join("query_term d", "d.doc_id = u.doc_id")
        query.join("query_term t", "t.doc_id = u.doc_id")
        query.where(f"u.path = '{self.U_PATH}'")
        query.where(f"d.path = '{self.D_PATH}'")
        query.where(f"d.value <= '{self.cutoff}'")
        query.where(f"t.path = '{self.T_PATH}'")
        query.where("t.value = 'Yes'")
        query.order("u.doc_id", "d.value DESC")
        rows = query.execute(self.cursor).fetchall()
        self.logger.info("found %d expiring links", len(rows))
        return rows

    @cached_property
    def recip(self):
        """Override of default recipients."""
        return self.opts.get("recip")

    @cached_property
    def report(self):
        """Email report."""

        if self.recip:
            recips = [self.recip]
        else:
            if self.test:
                group = "Test Publishing Notification"
            else:
                group = "Expiring Links Notification"
                recips = Job.get_group_email_addresses(group)
        if not recips:
            raise Exception("No recipients for report")
        subject = f"[{Tier().name}] Expiring links"
        self.logger.info("sending %s to %r", subject, recips)
        opts = dict(subject=subject, body=self.table, subtype="html")
        return EmailMessage(self.SENDER, recips, **opts)

    @cached_property
    def table(self):
        """HTML table for report."""

        style = "border: 1px solid #999; padding: .2em; margin: auto"
        B = builder
        rows = []
        for link in self.links:
            row = B.TR(
                B.TD(link.date, style=style),
                B.TD(f"CDR{link.doc_id}", style=style),
                B.TD(link.url or "", style=style),
            )
            rows.append(row)
        if not rows:
            message = "No links are scheduled to expire."
            rows = [B.TR(B.TD(message, colspan="3"))]
        today = utils.today().strftime("%Y-%m-%d")
        caption = f"Expiring Links Report - {today}"
        root = B.HTML(
            B.HEAD(
                B.META(charset="utf-8"),
                B.TITLE("Expiring Links"),
            ),
            B.BODY(
                B.TABLE(
                    B.THEAD(
                    B.CAPTION(caption, style=self.C_STYLE),
                        B.TR(
                            B.TH("Expires", style=style),
                            B.TH("CDR ID", style=style),
                            B.TH("URL", style=style),
                        ),
                    ),
                    B.TBODY(*rows),
                    style=self.T_STYLE,
                ),
            ),
        )
        opts = dict(
            pretty_print=True,
            encoding="utf-8",
            doctype="<!DOCTYPE html>",
        )
        return html.tostring(root, **opts)

    @cached_property
    def test(self):
        return self.opts.get("mode") != "live"

def main():
    """Make it possible to run this task from the command line."""

    parser = ArgumentParser()
    parser.add_argument("--mode", choices=("test", "live"), required=True)
    parser.add_argument("--recip", help="optional email address for testing")
    parser.add_argument("--cutoff", help="override age for expiration")
    opts = vars(parser.parse_args())
    ReportTask(None, "Expring Links", **opts).run()


if __name__ == "__main__":
    main()
