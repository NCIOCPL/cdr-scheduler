"""
Logic for Gov Delivery reports
"""

import cdr
import cdrdb2 as cdrdb
import datetime

from .cdr_task_base import CDRTask
from .task_property_bag import TaskPropertyBag

class ReportTask(CDRTask):
    """
    Implements subclass for managing scheduled GovDelivery reports.
    """

    LOGNAME = "gov_delivery_reports"

    def __init__(self, parms, data):
        """
        Initialize the base class then instantiate our Control object,
        which does all the real work. The data argument is ignored.
        """

        CDRTask.__init__(self, parms, data)
        self.control = Control(parms, self.logger)

    def Perform(self):
        "Hand off the real work to the Control object."
        self.control.run()
        return TaskPropertyBag()

class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.

    Class constants:

    TITLES          Map of report key to distinguishing part of report title.
    DEFAULT_START   Fall back on this for beginning of date range for report.
    DEFAULT_END     Fall back on this for end of date range.
    REPORTS         Full set of reports to be run by default (in order).
    SENDER          First argument to cdr.sendMail().
    CHARSET         Encoding used by cdr.sendMail().
    TSTYLE          CSS formatting rules for table elements.
    TO_STRING_OPTS  Options used for serializing HTML report object.
    CG              DNS name for this tier's Cancer.gov host.
    B               HTML builder module imported at Control class scope.
    HTML            HTML module imported at Control class scope.

    Instance properties:

    reports         Reports to be run in sequence specified.
    mode            Required report mode ("test" or "live").
    skip_email      If true, don't send report to recipients; just save it.
    start           Beginning of date range for selecting documents for report.
    end             End of date range for selecting documents for report.
    test            Convenience Boolean reflecting whether mode is 'test'.
    logger          Object for recording log information about the report.
    cursor          Object for submitting queries to the database.
    """

    import lxml.html.builder as B
    import lxml.html as HTML
    TITLES = {
        "trials": "Trials",
        "english": "New/Changed English Summaries",
        "spanish": "New/Changed Spanish Summaries",
    }
    REPORTS = ["english", "spanish", "trials"]
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
    TIER = cdr.Tier()
    CG = TIER.hosts["CG"]

    def __init__(self, options, logger):
        """
        Validate the settings:

        reports
            "english", "spanish", and/or "trials"; defaults to all three

        mode
            must be "test" or "live" (required); test mode restricts
            recipient list for report

        skip-email
            optional Boolean, defaults to False; if True, don't email
            the report to anyone

        log-level
            "info", "debug", or "error"; defaults to "info"

        start
            overrides the default start of the date range (a week ago)

        end
            overrides the default end of the date range (today)
        """

        self.TODAY = datetime.date.today()
        self.DEFAULT_END = self.TODAY - datetime.timedelta(1)
        self.DEFAULT_START = self.TODAY - datetime.timedelta(7)
        self.logger = logger
        self.logger.info("====================================")
        self.reports = options.get("reports") or self.REPORTS
        self.mode = options["mode"]
        self.skip_email = options.get("skip-email", False)
        self.start = options.get("start") or str(self.DEFAULT_START)
        self.end = options.get("end") or str(self.DEFAULT_END)
        self.test = self.mode == "test"
        self.cursor = cdrdb.connect("CdrGuest").cursor()
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

        title_args = (self.TITLES[key], self.start, self.end)
        self.title = "GovDelivery %s Report (%s to %s)" % title_args
        self.key = key
        report = self.create_report()
        self.logger.debug("report\n%s", report)
        if not self.skip_email:
            self.send_report(report)
        self.save_report(report)
        self.logger.info(self.title)

    def create_report(self):
        """
        Create an HTML document for one of this job's reports.

        The report on new trials deals with all of the new trials as a
        single result set, so we can hand off the generation of the
        report to the single TrialSet instance. The reports on
        summaries are broken down to show lots of subsets of the
        documents in separate tables, so we handle the logic here,
        instantiating as many SummarySet objects as we need (by
        calling the summary_table() method below).
        """

        if self.key == "trials":
            return TrialSet(self).report()
        style = "font-size: .9em; font-style: italic; font-family: Arial"
        body = self.B.BODY(
            self.B.H3(self.title, style="color: navy; font-family: Arial;"),
            self.B.P("Report date: %s" % datetime.date.today(), style=style)
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

        now = datetime.datetime.now().isoformat()
        stamp = now.split(".")[0].replace(":", "").replace("-", "")
        test = self.test and ".test" or ""
        name = "gd-%s-%s%s.html" % (self.key, stamp, test)
        path = "%s/reports/%s" % (cdr.BASEDIR, name)
        fp = open(path, "wb")
        fp.write(report)
        fp.close()
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

        if self.test:
            group = "Test Publishing Notification"
        else:
            group = {
                "spanish": "GovDelivery ES Docs Notification",
                "english": "GovDelivery EN Docs Notification",
                "trials": "GovDelivery Trials Notification"
            }.get(self.key)
        recips = CDRTask.get_group_email_addresses(group)
        if recips:
            subject = "[%s] %s" % (self.TIER.name, self.title)
            cdr.sendMailMime(self.SENDER, recips, subject, report, "html")
            self.logger.info("sent %s", subject)
            self.logger.info("recips: %s", ", ".join(recips))
        else:
            self.logger.error("no email recipients for %s", group)

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
            return cls.B.LI(cls.B.A(text, href=url, style="font-family: Arial"))
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
        s = ["%s:%s" % (k.replace("_", "-"), v) for k, v in d.items()]
        return ";".join(s)


class Trial:
    """
    Represents a single new trial for the report.

    nlm_id    NCT ID assigned by the National Library of Medicine (NLM).
    cdr_id    Document ID for our own repository.
    activated Date the trial began accepting patients.
    title     Brief title assigned to the trial.
    """

    TRIAL_VIEW = "about-cancer/treatment/clinical-trials/search/view"
    "Resource for trial link's URL."

    def __init__(self, nlm_id, cdr_id, activated, title):
        "Capture information needed to display this trial on the report."

        self.nlm_id = nlm_id
        self.cdr_id = cdr_id
        self.activated = str(activated)[:10]
        self.title = title

    def view_url(self):
        "Construct URL for viewing the trial information on cancer.gov."
        values = (Control.CG, self.TRIAL_VIEW, self.cdr_id)
        return "http://%s/%s?cdrid=%s" % values

    def tr(self):
        """
        Create the object for the row displaying this trial's information
        in a table.
        """

        url = self.view_url()
        return Control.B.TR(
            Control.td(self.nlm_id, url),
            Control.td(str(self.cdr_id)),
            Control.td(self.title, url),
            Control.td(self.activated, white_space="nowrap")
        )

    def li(self):
        """
        Create the object for the list item displaying this trial's
        information in an unordered list.
        """

        url = self.view_url()
        return Control.li(self.title, url)

    def __str__(self):
        "Display string for debugging."
        values = (self.nlm_id, self.cdr_id, self.activated, repr(self.title))
        return "%s (CDR%s) activated %s (%s)" % values


class TrialSet:
    """
    Set of all of the trials to be shown on this report.

        control   Object which has wrappers for using the lxml package's
                  factory methods to generate HTML elements.
        trials    Ordered sequence of Trial objects.
    """

    def __init__(self, control):
        "Create and execute the database query to collect the report's trials."
        self.control = control
        activated = "ISNULL(c.became_active, 0)"
        columns = ("c.nlm_id", "c.cdr_id", "c.became_active", "q.value")
        query = cdrdb.Query("ctgov_import c", *columns)
        query.join("pub_proc_cg p", "p.id = c.cdr_id")
        query.join("query_term_pub q", "q.doc_id = c.cdr_id")
        query.where("q.path = '/CTGovProtocol/BriefTitle'")
        query.where(query.Condition(activated, control.start, ">="))
        query.where(query.Condition(activated, control.end + " 23:59:59", "<="))
        control.logger.debug("query:\n%s", query)
        query.order("c.nlm_id").execute(control.cursor)
        rows = control.cursor.fetchall()
        control.logger.debug("%d rows", len(rows))
        self.trials = [Trial(*row) for row in rows]

    def report(self):
        """
        Returns an object for the report's HTML page.
        We show the data set twice: once as a table and then as a list.
        """

        if not self.trials:
            return "<p>No new trials.</p>"
        p_style = "font-size: .9em; font-style: italic; font-family: Arial"
        html = Control.B.HTML(
            self.control.html_head(),
            Control.B.BODY(
                Control.B.H3(self.control.title, style="font-family: Arial"),
                Control.B.P("Report date: %s" % datetime.date.today(),
                            style=p_style),
                self.table(),
                self.ul()
            )
        )
        return Control.serialize(html)

    def table(self):
        "Returns the object for the report's HTML table."
        style = "font-weight: bold; font-size: 1.2em; font-family: Arial;"
        style += "text-align: left;"
        table = Control.B.TABLE(
            Control.B.CAPTION("New Trials", style=style),
            Control.B.TR(
                Control.th("NCT ID"),
                Control.th("CDR ID"),
                Control.th("Title"),
                Control.th("Activated")
            ),
            style = Control.TSTYLE
        )
        for trial in self.trials:
            self.control.logger.debug(str(trial))
            table.append(trial.tr())
        return table

    def ul(self):
        "Returns the object for the report's unordered list."
        title = "Clinical Trials Now Accepting New Patients"
        ul = Control.B.UL()
        for trial in self.trials:
            ul.append(trial.li())
        ff = "font-family: Arial"
        return Control.B.DIV(
            Control.B.H3("Data from table above as a bulleted list", style=ff),
            Control.B.P(title, style="font-weight: bold; font-family: Arial"),
            ul
        )

class Summary:
    """
    Represents a single document for the report.

    cdr_id     Unique ID of the document in the CDR.
    title      Title extracted from the summary document.
    url        URL used to view the document on cancer.gov.
    fragment   Added to the URL to link to the "changes" section
               of the document on cancer.gov (only used for English
               'Summary' documents).
    """

    def __init__(self, cdr_id, title, url, fragment):
        "Capture the information needed to show this document on the report."
        self.cdr_id = cdr_id
        self.title = title
        self.url = url
        self.fragment = fragment

    def tr(self, summary_set):
        """
        Create the object for the row displaying this document's information
        in a table.

        summary_set   Provides additional information about the document.
        """

        frag_url = None
        if summary_set.doc_type == "Summary":
            if summary_set.audience == "Health professionals":
                frag_url = "%s#section/%s" % (self.url, self.fragment or "")
        return Control.B.TR(
            Control.td(str(self.cdr_id), self.url, width="10%"),
            Control.td(self.title, frag_url, width="90%")
        )

    def __str__(self):
        "Display string for debugging."
        url = self.url
        if self.fragment:
            url += "#section/%s" % self.fragment
        values = (self.cdr_id, url, repr(self.title))
        return "CDR%s (%s) %s" % values


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
        query = cdrdb.Query("document d", *columns).order("t.value").unique()

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
        query.where(query.Condition(date_val, control.end + " 23:59:59", "<="))

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
        rows = query.execute(control.cursor).fetchall()
        return [Summary(*row) for row in rows]

    def table(self):
        "Show a single summary results set for the report."
        style = "font-weight: bold; font-size: 1.2em; font-family: Arial"
        style += "; text-align: left;"
        table = Control.B.TABLE(
            Control.B.CAPTION(self.caption, style=style),
            style = Control.TSTYLE,
        )
        if self.summaries:
            headers = Control.B.TR(
                Control.th("CDR ID", width="10%"),
                Control.th("Title", width="90%"),
            )
            table.append(headers)
            for summary in self.summaries:
                self.control.logger.debug(str(summary))
                table.append(summary.tr(self))
        else:
            table.append(Control.B.TR(Control.td("None")))
        return table

def main():
    """
    Make it possible to run this task from the command line.
    You'll have to modify the PYTHONPATH environment variable
    to include the parent of this file's directory.
    """

    import argparse
    import logging
    fc = argparse.ArgumentDefaultsHelpFormatter
    desc = "Report on new/changed CDR documents for GovDelivery"
    reports = ["english", "spanish", "trials"]
    parser = argparse.ArgumentParser(description=desc, formatter_class=fc)
    parser.add_argument("--mode", choices=("test", "live"), required=True,
                        help="controls who gets the report")
    parser.add_argument("--skip-email", action="store_true",
                        help="just write the report to the file system")
    parser.add_argument("--log-level", choices=("info", "debug", "error"),
                        default="info", help="verbosity of logging")
    parser.add_argument("--reports", help="report(s) to run", nargs="*",
                        choices=Control.REPORTS, default=Control.REPORTS)
    parser.add_argument("--start", help="optional start of date range")
    parser.add_argument("--end", help="optional end of date range")
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    logging.basicConfig(format=cdr.Logging.FORMAT, level=args.log_level.upper())
    Control(opts, logging.getLogger()).run()

if __name__ == "__main__":
    main()
