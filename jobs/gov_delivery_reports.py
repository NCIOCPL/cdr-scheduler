"""Run the GovDelivery reports.
"""

from cdr import BASEDIR, EmailMessage, Logging
from cdrapi import db
from cdrapi.docs import Doc
from cdrapi.settings import Tier
from cdrapi.users import Session
from datetime import date, datetime, timedelta
from functools import cache, cached_property
from json import loads
from .base_job import Job

import sys


class ReportTask(Job):
    """
    Implements subclass for managing scheduled GovDelivery reports.
    """

    LOGNAME = "gov_delivery_reports"
    SUPPORTED_PARAMETERS = {
        "end",
        "mode",
        "recip",
        "reports",
        "skip-email",
        "start",
        "tier",
        "timeout",
    }

    def run(self):
        "Hand off the real work to the Control object."

        reports = self.opts.get("reports")
        if reports:
            try:
                reports = loads(reports)
            except Exception:
                if "," in reports:
                    reports = [r.strip() for r in reports.split(",")]
                else:
                    reports = [r.strip() for r in reports.split()]
            self.opts["reports"] = reports
        control = Control(self.opts, self.logger)
        control.run()


class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.

    Class constants:

    TITLES          Map of report key to distinguishing part of report title.
    REPORTS         Full set of reports to be run by default (in order).
    SENDER          First argument to EmailMessage constructor.
    CHARSET         Used in HTML page.
    TSTYLE          CSS formatting rules for table elements.
    TO_STRING_OPTS  Options used for serializing HTML report object.
    B               HTML builder module imported at Control class scope.
    HTML            HTML module imported at Control class scope.

    Instance properties:

    reports         Reports to be run in sequence specified.
    mode            Required report mode ("test" or "live").
    skip_email      If true, don't send report to recipients; just save it.
    start           Beginning of date range for selecting documents for report.
    end             End of date range for selecting documents for report.
    export_start    Date/time when export job starts. Will be used as default
                    start.
    recip           Override for who should get the report.
    test            Convenience Boolean reflecting whether mode is 'test'.
    logger          Object for recording log information about the report.
    cursor          Object for submitting queries to the database.
    tier            Object for the selected tier's settings.
    """

    import lxml.html.builder as B
    import lxml.html as HTML
    TITLES = {
        "english": "New/Changed English Summaries",
        "spanish": "New/Changed Spanish Summaries",
    }
    REPORTS = ["english", "spanish"]
    SENDER = "PDQ Operator <NCIPDQoperator@mail.nih.gov>"
    CHARSET = "utf-8"
    TSTYLE = (
        "width: 80%",
        "border: 1px solid #999",
        "border-collapse: collapse",
        "margin-top: 30px"
    )
    TSTYLE = "; ".join(TSTYLE)
    TO_STRING_OPTS = {
        "pretty_print": True,
        "encoding": CHARSET,
        "doctype": "<!DOCTYPE html>"
    }

    def __init__(self, options, logger):
        """
        Validate the settings:

        reports
            "english" and/or "spanish"; defaults to both

        mode
            must be "test" or "live" (required); test mode restricts
            recipient list for report

        skip-email
            optional Boolean, defaults to False; if True, don't email
            the report to anyone

        log-level
            "info", "debug", or "error"; defaults to "info"

        start
            overrides the default start of the date range (start of second
            to last weekly publishing job)

        end
            overrides the default end of the date range (start of last
            weekly publishing job)

        recip
            optional email address for testing so we don't spam others

        timeout
            how many seconds we'll wait for a connection or a query

        tier
            overrides the default of this server's local tier
        """

        # The default start/end used to be the week before today but has
        # changed to use the period between the last two weekly publishing
        # jobs
        #
        # default_start = date.today() - timedelta(7)
        # default_end = date.today() - timedelta(1)
        self.logger = logger
        self.logger.info("=" * 40)
        self.reports = options.get("reports") or self.REPORTS
        self.mode = options["mode"]
        self.skip_email = options.get("skip-email", False)
        self.test = self.mode == "test"
        self.tier = Tier(options.get("tier"))
        self.recip = options.get("recip")
        timeout = int(options.get("timeout", 300))
        opts = dict(user="CdrGuest", timeout=timeout, tier=self.tier.name)
        self.cursor = db.connect(**opts).cursor()

        # The report covers by default a full week and displays documents
        # published between the last weekly publishing job and the one
        # before that. This job used to run on a Sunday and include documents
        # for the week before that (Sunday through Saturday).  The publishing
        # job typically runs at 16:00h on Friday.  A document that is made
        # publishable after the Friday job started but before this report
        # starts on Sunday will incorrectly include such a document (assuming a
        # publishable version hasn't also been created during the specified
        # date range).  Therefore, in order to exclude such outliers,
        # we need to run the report from the previous-Friday-job through the
        # last Friday job. That start time is captured in self.export_start

        # Unless a date is specified the default date range covers the dates
        # between the last two successful 'Export' jobs.
        # ---------------------------------------------------------------------
        self.export_start, self.export_end = self.get_export_start_times()
        self.start = options.get("start") or str(self.export_start)
        self.end = options.get("end") or str(self.export_end)

        ##### For Testing #####
        # self.start = "2022-10-30"
        # self.end = "2022-11-05"
        ##### For Testing #####

        if self.skip_email:
            self.logger.info("skipping email of reports")

    def run(self):
        "Run each of the reports we've been asked to create."

        for key in self.reports:
            try:
                self.do_report(key)
            except Exception as e:
                self.logger.exception("do_report(%s): %s", key, e)
        self.logger.info("%s job completed", self.mode)

    def do_report(self, key):
        """
        Create, save, and (optionally) send out a single report.

        key       Identifies which report we should process.
                  See Control.REPORTS for expected values.
        """

        title_args = (self.TITLES[key], self.start.split()[0],
                      self.end.split()[0])
        self.title = "GovDelivery %s Report (%s to %s)" % title_args
        self.sub_title = "Date/Time: (%s to %s)" % (self.export_start,
                                                     self.export_end)
        self.logger.info(self.title)      ## Testing
        self.logger.info(self.sub_title)  ## Testing

        self.key = key
        report = self.create_report()
        self.logger.debug("report\n%s", report)

        if not self.skip_email:
            self.send_report(report)

        self.save_report(report)


    def create_report(self):
        """
        Create an HTML document for one of this job's reports.

        The reports on summaries are broken down to show lots of
        subsets of the documents in separate tables, so we handle the
        logic here, instantiating as many SummarySet objects as we
        need (by calling the summary_table() method below).
        """

        style = "font-size: .9em; font-style: italic; font-family: Arial"
        body = self.B.BODY(
            self.B.H3(self.title, style="color: navy; font-family: Arial;"),
            self.B.P("Adjusted date range: %s" % self.sub_title, style=style),
            self.B.P("Report date: %s" % date.today(), style=style)
        )
        for audience in ("Health professionals", "Patients"):
            body.append(self.summary_table("Summary", True, audience))
            body.append(self.summary_table("Summary", False, audience))
        if self.key == "english":
            body.append(self.summary_table("DrugInformationSummary", True))
            body.append(self.summary_table("DrugInformationSummary", False))
        return self.serialize(self.B.HTML(self.html_head(), body))


    def summary_table(self, doc_type, new, audience=None):
        """
        Create a SummarySet instance to generate the table for a slice
        of the documents in the report.

        doc_type    Either "Summary" or "DrugInformationSummary."
        new         If true, find documents first published in the
                    date range. Otherwise, find documents whose
                    DateLastModified value falls withing this range.
        audience    Either "Health professionals" or "Patients"
                    (only used for summaries).
        """

        args = {
            "doc_type": doc_type,
            "new": new,
            "audience": audience
        }
        if doc_type == "Summary":
            args["language"] = self.key.capitalize()
        return SummarySet(self, **args).table()


    def save_report(self, report):
        """
        Write the generated report to the cdr/reports directory.

        report    Serialized HTML document for the report.
        """

        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        test = ".test" if self.test else ""
        name = f"gd-{self.key}-{stamp}{test}.html"
        path = f"{BASEDIR}/reports/{name}"
        with open(path, "wb") as fp:
            fp.write(report)
        self.logger.info("created %s", path)

    def html_head(self):
        "Common code to create the top part of the generated report."
        return self.B.HEAD(
            self.B.META(charset=self.CHARSET),
            self.B.TITLE(self.title),
        )

    def send_report(self, report):
        """
        Email the report to the right recipient list.

        report    Serialized HTML document for the report.
        """

        if self.recip:
            recips = [self.recip]
        else:
            if self.test:
                group = "Test Publishing Notification"
            else:
                group = {
                    "spanish": "GovDelivery ES Docs Notification",
                    "english": "GovDelivery EN Docs Notification",
                }.get(self.key)
                recips = Job.get_group_email_addresses(group)
        if recips:
            subject = f"[{self.tier.name}] {self.title}"
            opts = dict(subject=subject, body=report, subtype="html")
            message = EmailMessage(self.SENDER, recips, **opts)
            message.send()
            self.logger.info("sent %s", subject)
            self.logger.info("recips: %s", ", ".join(recips))
        else:
            self.logger.error("no email recipients for %s", group)

    @cached_property
    def session(self):
        """Guest session for fetching documents."""
        return Session("guest", tier=self.tier.name)

    @classmethod
    def th(cls, label, **styles):
        """
        Helper method to generate a table column header.

        label      Display string for the column header
        styles     Optional style tweaks. See merge_styles() method.
        """

        default_styles = {
            "font-family": "Arial",
            "border": "1px solid #999",
            "margin": "auto",
            "padding": "2px",
        }
        style = cls.merge_styles(default_styles, **styles)
        return cls.B.TH(label, style=style)

    @classmethod
    def td(cls, data, url=None, **styles):
        """
        Helper method to generate a table data cell.

        data       Data string to be displayed in the cell
        styles     Optional style tweaks. See merge_styles() method.
        """

        default_styles = {
            "font-family": "Arial",
            "border": "1px solid #999",
            "vertical-align": "top",
            "padding": "2px",
            "margin": "auto"
        }
        style = cls.merge_styles(default_styles, **styles)
        if url:
            return cls.B.TD(cls.B.A(data, href=url), style=style)
        elif isinstance(data, (list, tuple)):
            return cls.B.TD(*data, style=style)
        return cls.B.TD(data, style=style)

    @classmethod
    def li(cls, text, url=None):
        """
        Helper method for creating a list item element.

        text       Display string for the list item.
        url        Optional URL, causing the text to be wrapped
                   in a link element.
        """

        if url:
            opts = dict(href=url, style="font-family: Arial")
            return cls.B.LI(cls.B.A(text, **opts))
        return cls.B.LI(text, style="font-family: Arial")

    @classmethod
    def serialize(cls, html):
        """
        Create a properly encoded string for the report.

        html       Tree object created using lxml HTML builder.
        """

        return cls.HTML.tostring(html, **cls.TO_STRING_OPTS)

    @staticmethod
    def merge_styles(defaults, **styles):
        """
        Allow the default styles for an element to be overridden.

        defaults   Dictionary of style settings for a given element.
        styles     Dictionary of additional or replacement style
                   settings. If passed as separate arguments the
                   setting names with hyphens will have to have been
                   given with underscores instead of hyphens. We
                   restore the names which CSS expects.
        """

        d = dict(defaults, **styles)
        s = [f"""{k.replace("_", "-")}:{v}""" for k, v in d.items()]
        return ";".join(s)


    def get_export_start_times(self):
        """
        Get the start times of the previous 2 Export jobs. Those are the
        immediate successful Export jobs before a successful Push job.
        We're only interested to display documents that did get published
        to Cancer.gov.  Therefore, we have to ensure the documents were
        pushed and not just exported.
        We then will identify the timestamp for the corresponding Export jobs.
        """
        # Select the last 2 publishing job times of a successful job
        query = db.Query("pub_proc", "top 2 id").order("id DESC")
        query.where("status = 'Success'")
        query.where("pub_subset = 'Push_Documents_To_Cancer.gov_Export'")

        # self.logger.info(query)

        rows = query.execute(self.cursor).fetchall()
        # self.logger.info(rows)

        date_range = []
        for job_id, *row in rows:
            query = db.Query("pub_proc", "top 1 MAX(id), started")
            query.where("status = 'Success'")
            query.where("pub_subset = 'Export'")
            query.where(f"id < {job_id}")
            query.group("started").order("started DESC")

            row = query.execute(self.cursor).fetchone()
            date_range.append(row[1])

        # The SQL query gives us the last job first, so we reverse the order
        # here to get the start date for the job first again.
        if date_range: date_range.reverse()
        return [value.strftime('%Y-%m-%d %H:%M:%S') for value in date_range]


class Summary:
    """
    Represents a single document for the report.

    summary_set Access to selection criteria for set containing this summary
    control     Access to runtime information we need (e.g., the tier)
    cdr_id      Unique ID of the document in the CDR.
    title       Title extracted from the summary document.
    url         URL used to view the document on cancer.gov.
    fragment    Added to the URL to link to the "changes" section
                of the document on cancer.gov (only used for English
                'Summary' documents).
    """

    CHANGES = "SummarySection[SectMetaData/SectionType='Changes to summary']"
    ORG_NAME = "/Organization/OrganizationNameInformation/OfficialName/Name"
    BOARD = "PDQ Adult Treatment Editorial Board"
    BOARD = "/Summary/SummaryMetaData/PDQBoard/Board/@cdr:ref"
    EDITORIAL_CHANGES = (
        "Editorial changes were made to this summary",
        "Se incorporaron cambios editoriales en este resumen",
    )

    def __init__(self, summary_set, cdr_id, title, url, fragment):
        "Capture the information needed to show this document on the report."

        self.summary_set = summary_set
        self.control = summary_set.control
        self.cdr_id = cdr_id
        self.title = title
        self.url = url
        self.fragment = fragment or ""

    def __lt__(self, other):
        """Sort based on calculated key.

        Pass:
            other - reference to object to which this one is being compared

        Return:
            True if this object should come before the other object
        """

        return self.key < other.key

    def __str__(self):
        "Display string for debugging."

        url = self.url
        if self.fragment:
            url += f"#section/{self.fragment}"
        return f"CDR{self.cdr_id} ({url}) {self.title!r}"

    @cached_property
    def board(self):
        """Board name (if we have one)."""

        if self.summary_set.doc_type != "Summary":
            return ""
        query = db.Query("query_term n", "n.value")
        query.join("query_term b", "b.int_val = n.doc_id")
        query.where(f"b.path = '{self.BOARD}'")
        query.where(f"n.path = '{self.ORG_NAME}'")
        query.where("n.value LIKE 'PDQ%Editorial Board'")
        if self.summary_set.language == "English":
            query.where(query.Condition("b.doc_id", self.cdr_id))
        else:
            query.join("query_term t", "t.int_val = b.doc_id")
            query.where("t.path = '/Summary/TranslationOf/@cdr:ref'")
            query.where(query.Condition("t.doc_id", self.cdr_id))
        rows = query.execute(self.control.cursor).fetchall()
        if not rows:
            return ""
        board = rows[0].value.replace("PDQ", "").replace("Editorial Board", "")
        return board.strip()

    @cached_property
    def change_blocks(self):
        """'Changes to this Summary' section (should be only one)."""
        try:
            return self.doc.root.xpath(self.CHANGES)
        except Exception:
            self.control.logger.exception("document %s", self.cdr_id)
            return []

    @cached_property
    def changes(self):
        """Get information for last column in HP summary tables.

        "As discussed, we would like to modify the weekly English and
        Spanish GovDelivery reports of new/changed summaries to add a third
        column to the New and Revised Health Professional Summaries tables
        displaying the titles of the subsections that include changes
        highlighted in the changes section.

        To populate this new column (which can be named "Section(s)") with
        data from the last publishable version of the summary, the software
        should:

        1. locate the "Changes to summary" section (from the section metadata)
        2. Identify text that is in both strong & para tags (our convention
           is to use strong & para tags to represent headings of sections
           containing changes. If we used section tags, these would show up as
           sections in the table of contents. I am sure this is very
           consistently applied.)
        3. Capture this text and display it on the report
        4. If there isn't any text in the Changes to summary section that
           fits #2 above (strong & para tags), please display the complete
           string of text in the Changes to summary section.

        I will attach an example.

        If possible, we would like to complete this change by Nov 1."

        See enhancement request OCECDR-5143.

        Read all the comments in the ticket to find all the ways in which
        the original requirements were changed.
        """

        style = "margin: 0 3px 1rem;"
        for block in self.change_blocks:
            for para in block.iter("Para"):
                for strong in para.iter("Strong"):
                    section = Doc.get_text(strong, "").strip()
                    if section:
                        changes.append(Control.B.P(section, style=style))
        if changes:
            return changes
        for block in self.change_blocks:
            for metadata in block.iter("SectMetaData"):
                block.remove(metadata)
            for title in block.iter("Title"):
                block.remove(title)
            for para in block.iter("Para"):
                block.remove(para)
                break
            text = Doc.get_text(block, "").strip()
            if text:
                changes.append(Control.B.P(text, style=style))
        return changes

    @cached_property
    def doc(self):
        """API Object representing the CDR Summary document."""
        return Doc(self.control.session, id=self.cdr_id, version="lastp")

    @cached_property
    def editorial_changes(self):
        """True if only editorial changes have been made."""

        for block in self.change_blocks:
            text = Doc.get_text(block, "")
            for phrase in self.EDITORIAL_CHANGES:
                if phrase in text:
                    return True
        return False

    @cached_property
    def key(self):
        """Sort by board (if we have one) and then by title."""
        return self.board, self.title.lower()

    @cached_property
    def tr(self):
        """
        Create the object for the row displaying this document's information
        in a table.

        summary_set   Provides additional information about the document.
        """

        frag_url = None
        changed_hp = False
        columns = [Control.td(str(self.cdr_id), self.url)]
        if self.summary_set.audience == "Health professionals":
            frag_url = f"{self.url}#{self.fragment}"
            columns.append(Control.td(self.title, frag_url))
            columns.append(Control.td(self.board))
            if not self.summary_set.new:
                columns.append(Control.td(self.changes or ""))
        else:
            columns.append(Control.td(self.title))
        return Control.B.TR(*columns)


class SummarySet:
    """
    results set of summaries representing a slice of the report.

    control       Object controlling report logic, database access,
                  and HTML generation.
    doc_type      Document type ("Summary" or "DrugInformationSummary").
    new           If True, find documents first published in the report's
                  date range. Otherwise, find documents whose DateLastModified
                  value falls within that range.
    language      "English" or "Spanish" (not used for Drug Information
                  Summaries).
    audience      "Health professionals" or "Patients" (not used for
                  Drug Information Summaries).
    caption       String used to identify which results set this is.
    """

    def __init__(self, control, **kwargs):
        "Extract the options used to construct this results set."
        self.control = control
        self.doc_type = kwargs.get("doc_type")
        self.new = kwargs.get("new")
        self.language = kwargs.get("language")
        self.audience = kwargs.get("audience")
        self.summaries = self.get_summaries(control)
        self.caption = self.make_caption()

    def make_caption(self):
        "Assemble the string used to identify the results set."
        caption = self.new and "New " or "Revised "
        if self.doc_type == "Summary":
            if self.audience == "Patients":
                caption += "Patient Summaries"
            else:
                caption += "Health Professional Summaries"
                if self.new:
                    caption += f" ({len(self.summaries)})"
                else:
                    count = 0
                    for summary in self.summaries:
                        if not summary.editorial_changes:
                            count += 1
                    caption += f" ({count}, excluding editorial changes)"
        else:
            caption += "Drug Information Summaries"
        self.control.logger.debug("%d %s", len(self.summaries), caption)
        return caption

    def get_summaries(self, control):
        """
        Fetch the summaries for one slice of the report.

        control    Object used for logging, database access, and to
                   remember the date range used for the report.
        """

        is_new = "New " if self.new else ""
        if self.audience:
            args = is_new, self.doc_type, self.audience
            self.control.logger.debug(f"%s%s - %s", *args)

        # Set paths here so we can avoid super-long code lines.
        l_path = "/Summary/SummaryMetaData/SummaryLanguage"
        a_path = "/Summary/SummaryMetaData/SummaryAudience"
        s_path = "/Summary/SummarySection/SectMetaData/SectionType"
        f_path = "/Summary/SummarySection/@cdr:id"
        if self.doc_type == "Summary":
            t_path = "/Summary/SummaryTitle"
            u_path = "/Summary/SummaryMetaData/SummaryURL/@cdr:xref"
        else:
            t_path = "/DrugInformationSummary/Title"
            u_path = "/DrugInformationSummary/DrugInfoMetaData/URL/@cdr:xref"

        # For summaries we need a fourth column for fragment links.
        f_col = self.doc_type == "Summary" and "f.value" or "NULL as dummy"
        columns = ["d.id", "t.value", "u.value", f_col]

        # Create a new query against the document table.
        query = db.Query("document d", *columns).order("t.value").unique()

        # Make sure the document is active and currently published.
        query.where("d.active_status = 'A'")
        query.join("pub_proc_cg c", "c.id = d.id")

        # Add a join to get the document's title.
        query.join("query_term_pub t", "t.doc_id = d.id")
        query.where(query.Condition("t.path", t_path))

        # Another join to get the URL for linking to the doc on cancer.gov.
        query.join("query_term_pub u", "u.doc_id = d.id")
        query.where(query.Condition("u.path", u_path))

        # Test to see if the creation or modification of the doc is in range.
        if not self.new:
            query.outer("query_term_pub m", "m.doc_id = d.id",
                        "m.path = '/%s/DateLastModified'" % self.doc_type)
        date_val = "ISNULL(%s, 0)" % (self.new and "d.first_pub" or "m.value")
        query.where(query.Condition(date_val, control.start, ">="))
        query.where(query.Condition(date_val, control.end, "<="))

        # For summaries we do each audience separately.
        if self.audience:
            query.join("query_term_pub a", "a.doc_id = d.id")
            query.where(query.Condition("a.path", a_path))
            query.where(query.Condition("a.value", self.audience))

        # Each report is language-specific for Summary documents.
        if self.language:
            query.join("query_term_pub l", "l.doc_id = d.id")
            query.where(query.Condition("l.path", l_path))
            query.where(query.Condition("l.value", self.language))

        # For HP Summary documents we need a fragment link to the changes.
        if self.doc_type == "Summary":
            query.outer("query_term_pub s", "s.doc_id = d.id",
                        "s.path = '%s'" % s_path,
                        "s.value = 'Changes to summary'")
            query.outer("query_term_pub f", "f.doc_id = d.id",
                        "LEFT(f.node_loc, 4) = LEFT(s.node_loc, 4)",
                        "f.path = '%s'" % f_path)

        # If we're debugging log the query string.
        control.logger.debug(query)

        # Fetch the documents and pack up a sequence of Summary objects.
        start = datetime.now()
        rows = query.execute(control.cursor).fetchall()
        args = len(rows), datetime.now() - start
        control.logger.debug("get_summaries(): %d rows in %s", *args)

        summaries = []
        control.logger.debug(rows)

        for row in rows:
            id = row[0]
            # Check if latest pub version of summary was created after
            # the last publishing job started
            if self.late_pubversion(control, id):
                if self.is_published(control, id):
                    summaries.append(row)
                continue
            summaries.append(row)

        if summaries:
            return [Summary(self, *row) for row in summaries]
        return summaries


    def late_pubversion(self, control, doc_id):
        """
        Test if this document should be included in the output based on
        the version number.
        If the last publishable version has been created *after* the last
        last publishing job ran this document will have to be excluded.
        Note:
        If a publishable version exists that was created after the weekly
        publishing job started but there also exists an earlier publishable
        version created within the specified date range, we will still
        exclude the document because the DateLastModified cannot be
        correctly determined.  The query_term_pub table used to retrieve
        the DLM will only show the value for the latest publishable version.
        We would have to retrieve the version's xml document and inspect
        the DLM date since the query_term table only stores the info from
        the latest version.
        """

        # Does a publishable version exist that was created after the
        # publishing job started?
        subq = db.Query("pub_proc_doc pd", "max(doc_version)")
        subq.join("pub_proc pp", "pp.id = pd.pub_proc")
        subq.where(f"doc_id = {doc_id}")
        subq.where("o.id = pd.doc_id")
        subq.where("failure IS NULL")
        subq.where("pub_subset like 'Push%Export'")

        query = db.Query("doc_version o", "num", "dt")
        query.where(query.Condition("num", subq, ">"))
        query.where("publishable = 'Y'")

        control.logger.debug(query)  ## Testing

        row = query.execute(control.cursor).fetchone()

        if row:
            # Found a publishable version created too late to be included
            return True
        # The publishable version was created within specified date reange
        return False


    def is_published(self, control, doc_id):
        """
        It has been identified that the latest publishable version of
        this document has been created too late to be included.
        However, it's still possible an earlier version of the document was
        included in the publishing run and should still be used.
        Check if the previous publishable version was created within the
        required date range. In that case the document will be included,
        otherwise is will be excluded.
        Note:
        This test does not cover all possible scenarios but other it has
        been decided not to prevent additional edge cases of the given
        edge case we're trying to address.
        """

        # Does a publishable version exist that was created after the
        # publishing job started?
        # Retrieve the version number and creation date for that version
        subq = db.Query("pub_proc_doc pd", "max(doc_version)")
        subq.join("pub_proc pp", "pp.id = pd.pub_proc")
        subq.where(f"doc_id = {doc_id}")
        subq.where("o.id = pd.doc_id")
        subq.where("failure IS NULL")
        subq.where("pub_subset like 'Push%Export'")

        query = db.Query("doc_version o", "num", "dt")
        query.where(query.Condition("num", subq, "="))
        query.where("publishable = 'Y'")

        control.logger.debug(query)  ## Testing

        row = query.execute(control.cursor).fetchone()

        control.logger.info(f"*** Pub version for {doc_id} created after "
                             "job started!!!")
        control.logger.info("*** Inspection previous pub version")
        control.logger.info(row)

        dformat = "%Y-%m-%d %H:%M:%S"
        dt_start = datetime.strptime(control.start, dformat)
        dt_end = datetime.strptime(control.end, dformat)

        if row and dt_start < row[1] and dt_end > row[1]:
            # This version matches our date range
            return True
        # The version was published outside our date range
        return False


    def table(self):
        "Show a single summary results set for the report."
        style = "font-weight: bold; font-size: 1.2em; font-family: Arial"
        style += "; text-align: left;"
        table = Control.B.TABLE(
            Control.B.CAPTION(self.caption, style=style),
            style=Control.TSTYLE,
        )
        if self.summaries:
            summaries = self.summaries
            headers = [Control.th("CDR ID"), Control.th("Title")]
            if self.audience == "Health professionals":
                summaries = sorted(self.summaries)
                headers.append(Control.th("Board"))
                if not self.new:
                    headers.append(Control.th("Section(s)"))
            headers = Control.B.TR(*headers)
            table.append(headers)
            for summary in summaries:
                self.control.logger.debug(str(summary))
                table.append(summary.tr)
        else:
            table.append(Control.B.TR(Control.td("None")))
        return table


def main():
    """
    Make it possible to run this task from the command line.

    For usage information, enter

        python -m jobs.gov_delivery_reports --help

    from the scheduler directory.
    """

    import argparse
    import logging
    fc = argparse.ArgumentDefaultsHelpFormatter
    desc = "Report on new/changed CDR documents for GovDelivery"
    parser = argparse.ArgumentParser(description=desc, formatter_class=fc)
    parser.add_argument("--mode", choices=("test", "live"), required=True,
                        help="controls who gets the report")
    parser.add_argument("--skip-email", action="store_true",
                        help="just write the report to the file system")
    parser.add_argument("--log-level", choices=("info", "debug", "error"),
                        default="info", help="verbosity of logging")
    parser.add_argument("--reports", help="report(s) to run", nargs="*",
                        choices=Control.REPORTS, default=Control.REPORTS[:2])
    parser.add_argument("--start", help="optional start of date range")
    parser.add_argument("--end", help="optional end of date range")
    parser.add_argument("--recip", help="optional email address for testing")
    parser.add_argument("--tier", help="override default tier")
    parser.add_argument("--timeout", type=int, default=300,
                        help="how many seconds to wait for SQL Server")
    args = parser.parse_args()
    opts = dict(format=Logging.FORMAT, level=args.log_level.upper())
    logging.basicConfig(**opts)
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    Control(opts, logging.getLogger()).run()


if __name__ == "__main__":
    main()
