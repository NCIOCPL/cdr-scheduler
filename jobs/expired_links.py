"""Remove temporary blog links which have passed their "sell-by" date.
"""

from argparse import ArgumentParser
from functools import cached_property
from dateutil import relativedelta, utils
from lxml import etree
from .base_job import Job as ScheduledJob
from cdrapi import db
from cdrapi.docs import Doc, Link
from cdrapi.settings import Tier
from cdrapi.users import Session
from ModifyDocs import Job as GlobalChangeJob


class LinkRemover(ScheduledJob):
    """
    Implements subclass for managing the scheduled link cleanup job.
    """

    LOGNAME = "remove-expired-links"
    SUPPORTED_PARAMETERS = {
        "mode",
        "cutoff",
    }
    DRUG_REFERENCE = "/DrugInformationSummary/DrugReference"
    U_PATH = f"{DRUG_REFERENCE}/DrugReferenceLink/@cdr:xref"
    D_PATH = f"{DRUG_REFERENCE}/DrugReferencePostedDate"
    T_PATH = f"{DRUG_REFERENCE}/@TemporaryLink"

    def run(self):
        """Find the old links and report them."""
        Updater(self).run()

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
        then = now - relativedelta.relativedelta(years=+3)
        return then.strftime("%Y-%m-%d")

    @cached_property
    def ids(self):
        """CDR IDs of documents with expired links."""

        fields = "u.doc_id", "u.value AS url", "d.value AS date"
        query = db.Query("query_term u", "u.doc_id").unique()
        query.join("query_term d", "d.doc_id = u.doc_id")
        query.join("query_term t", "t.doc_id = u.doc_id")
        query.where(f"u.path = '{self.U_PATH}'")
        query.where(f"d.path = '{self.D_PATH}'")
        query.where(f"d.value <= '{self.cutoff}'")
        query.where(f"t.path = '{self.T_PATH}'")
        query.where("t.value = 'Yes'")
        query.order("u.doc_id")
        rows = query.execute(self.cursor).fetchall()
        self.logger.info("found %d documents with expiring links", len(rows))
        return [row.doc_id for row in rows]

    @cached_property
    def test(self):
        return self.opts.get("mode") != "live"


class Updater(GlobalChangeJob):
    """Global change job used to remove expired links."""

    LOGNAME = "remove-expired-links"
    COMMENT = "Removing expired temporary links"
    ACCOUNT = "linksweeper"
    MESSAGE = "removing DrugRefrence block from CDR%s added %s for URL %r"

    def __init__(self, control, /):
        """Capture the caller's values.

        Pass:
            control - access to document ID list, cutoff date, and run mode
        """

        self.__control = control
        mode = "test" if control.test else "live"
        opts = dict(mode=mode, console=False)
        GlobalChangeJob.__init__(self, **opts)

    def select(self):
        """Return sequence of CDR ID integers for documents to transform."""
        return self.__control.ids

    def transform(self, doc):
        """Refresh the CDR document with values from the EVS concept.

        Pass:
            doc - reference to `cdr.Doc` object

        Return:
            serialized XML for the modified document
        """

        # Find and remove any stale links.
        int_id = Doc.extract_id(doc.id)
        root = etree.fromstring(doc.xml)
        for node in root.iter("DrugReference"):
            if node.get("TemporaryLink") == "Yes":
                posted = Doc.get_text(node.find("DrugReferencePostedDate"))
                if posted and posted < self.__control.cutoff:
                    link = node.find("DrugReferenceLink")
                    url = "" if link is None else link.get(Link.CDR_XREF)
                    self.logger.info(self.MESSAGE, int_id, posted, url or "")
                    node.getparent().remove(node)
        return etree.tostring(root)

    @cached_property
    def session(self):
        """Session for account created specifically for this job."""

        password = Tier().passwords.get(self.ACCOUNT)
        if not password:
            raise Exception("Link cleanup account not found")
        session = Session.create_session(self.ACCOUNT, password=password)
        return str(session)


def main():
    """Make it possible to run this task from the command line."""

    parser = ArgumentParser()
    parser.add_argument("--mode", choices=("test", "live"), required=True)
    parser.add_argument("--cutoff", help="override age for expiration")
    opts = vars(parser.parse_args())
    LinkRemover(None, "Link Cleanup", **opts).run()


if __name__ == "__main__":
    main()
