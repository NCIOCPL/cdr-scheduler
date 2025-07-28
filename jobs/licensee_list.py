"""
Program to create a list of active licensees (Production/Test)
This job should run as a scheduled job once a month.
"""

import datetime
import requests
import cdr
from cdrapi import db
from .base_job import Job


class ReportTask(Job):
    """
    Implements subclass for managing the monthly licensee report.
    """

    LOGNAME = "licensees"
    SUPPORTED_PARAMETERS = {"mode", "recip", "skip-email"}

    def run(self):
        "Hand off the real work to the Control object."
        control = Control(self.opts, self.logger)
        control.run()


class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.

    Class constants:

    TITLE           Name of the report
    SENDER          First argument to cdr.EmailMessage constructor.
    CHARSET         Encoding used by HTML page.
    TSTYLE          CSS formatting rules for table elements.
    TO_STRING_OPTS  Options used for serializing HTML report object.
    B               HTML builder module imported at Control class scope.
    HTML            HTML module imported at Control class scope.

    Instance properties:

    mode            Required report mode ("test" or "live").
    skip_email      If true, don't send report to recipients; just save it.
    test            Convenience Boolean reflecting whether mode is 'test'.
    logger          Object for recording log information about the report.
    cursor          Object for submitting queries to the database.
    """

    import lxml.html.builder as B
    import lxml.html as HTML
    TITLE = "List of PDQ Content Distribution Partners"
    MODES = "test", "live"
    SENDER = "PDQ Operator <NCIPDQoperator@mail.nih.gov>"
    CHARSET = "ascii"
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
        Save the logger object and extract and validate the settings:

        mode
            must be "test" or "live" (required); test mode restricts
            recipient list for report

        recip
            optional email address, used when testing so we don't
            spam anyone else

        skip-email
            optional Boolean, defaults to False; if True, don't email
            the report to anyone

        log-level
            "info", "debug", or "error"; defaults to "info"
        """

        self.options = options
        self.mode = options["mode"]
        self.recip = options.get("recip")
        self.skip_email = options.get("skip-email") or False
        self.test = self.mode == "test"
        self.logger = logger
        if self.mode not in self.MODES:
            raise Exception("invalid mode %s" % repr(self.mode))
        self.cursor = db.connect(user="CdrGuest").cursor()

    def run(self):
        """
        Create the report, optionally email it, and save it to
        a file in the reports directory.
        """

        self.logger.info("%s job started", self.mode)
        self.logger.info("options: %s", self.options)
        report = self.create_report()
        if not self.skip_email:
            self.send_report(report)
        self.save_report(report)
        self.logger.info("%s job completed", self.mode)

    def create_report(self):
        """
        Create the object for the report's HTML document and serialize it.
        Most of the work is handled by the Partners class, which assembles
        the report's table.
        """

        pstyle = "font-size: .9em; font-style: italic; font-family: Arial"
        report_date = datetime.date.today()
        html = self.B.HTML(
            self.B.HEAD(
                self.B.META(charset=self.CHARSET),
                self.B.TITLE(self.TITLE),
            ),
            self.B.BODY(
                self.B.H3(self.TITLE, style="font-family: Arial"),
                self.B.P(f"Report date: {report_date}", style=pstyle),
                Partners(self).table()
            )
        )
        return self.serialize(html)

    def save_report(self, report):
        """
        Write the generated report to the cdr/reports directory.

        report    Serialized HTML document for the report.
        """

        now = datetime.datetime.now().isoformat()
        stamp = now.split(".")[0].replace(":", "").replace("-", "")
        name = "licensees-%s.html" % stamp
        path = "%s/reports/%s" % (cdr.BASEDIR, name)
        fp = open(path, "wb")
        fp.write(report)
        fp.close()
        self.logger.info("created %s", path)

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
                group = "Licensee Report Notification"
            recips = Job.get_group_email_addresses(group)
        title = "PDQ Distribution Partner List"
        subject = "[%s] %s" % (cdr.Tier().name, title)
        opts = dict(subject=subject, body=report, subtype="html")
        message = cdr.EmailMessage(self.SENDER, recips, **opts)
        message.send()
        self.logger.info("sent %s", subject)
        self.logger.info("recips: %s", ", ".join(recips))

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
            "padding": "2px 5px",
            "margin": "auto"
        }
        if data is None:
            data = ""
        style = cls.merge_styles(default_styles, **styles)
        if url:
            return cls.B.TD(cls.B.A(str(data), href=url), style=style)
        return cls.B.TD(data, style=style)

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


class Partners:
    """
    Set of all of the PDQ data partners to be shown on this report.

        control   Object which has wrappers for using the lxml package's
                  factory methods to generate HTML elements.
        trials    Ordered sequence of Partner objects.
    """

    INFO = "/Licensee/LicenseeInformation"
    NAME = "%s/LicenseeNameInformation/OfficialName/Name" % INFO
    STATUS = "%s/LicenseeStatus" % INFO
    DATES = "%s/LicenseeStatusDates" % INFO
    TEST_START = "%s/TestActivation" % DATES
    TEST_EXTENSION = "%s/TestExtension" % DATES
    TEST_END = "%s/TestInactivation" % DATES
    PROD_START = "%s/ProductionActivation" % DATES
    PROD_END = "%s/ProductionInactivation" % DATES
    USER_NAME = "/Licensee/FtpInformation/UserName"
    test_count = 0
    prod_count = 0

    def __init__(self, control):
        """
        Collect the Partner document objects.
        """

        # Zero out the counts.
        Partners.test_count = Partners.prod_count = 0

        # Fetch information about when each account last fetched data.
        url = ("https://cdr-dev.cancer.gov"
               "/cgi-bin/cdr/last-pdq-data-partner-accesses.py")
        control.logger.info("fetching contacts from %r", url)
        self.last_access = dict()
        for line in requests.get(url).text.splitlines():
            fields = line.split()
            self.last_access[fields[0]] = fields[1]

        # Create the database query to fetch the licensee information.
        cols = ("n.doc_id", "n.value AS name", "s.value AS status",
                "ta.value AS test_activation",
                "te.value AS test_extension",
                "ti.value AS test_inactivation",
                "pa.value AS prod_activation",
                "pi.value AS prod_inactivation",
                "un.value AS user_name")
        query = db.Query("query_term n", *cols)

        # Get the licensee's name.
        query.where("n.path = '%s'" % self.NAME)

        # Get the licensee's status.
        query.join("query_term s", "s.doc_id = n.doc_id")
        query.where("s.path = '%s'" % self.STATUS)
        query.where("s.value IN ('Test', 'Production')")

        # Find out when they started the test phase.
        query.join("query_term ta", "ta.doc_id = n.doc_id")
        query.where("ta.path = '%s'" % self.TEST_START)

        # Find out if/when they got a continuation of the test phase.
        query.outer("query_term te", "te.doc_id = n.doc_id",
                    "te.path = '%s'" % self.TEST_EXTENSION)

        # When did the test phase end?
        query.outer("query_term ti", "ti.doc_id = n.doc_id",
                    "ti.path = '%s'" % self.TEST_END)

        # When did they go into production?
        query.outer("query_term pa", "pa.doc_id = n.doc_id",
                    "pa.path = '%s'" % self.PROD_START)

        # Have they been turned off from production?
        query.outer("query_term pi", "pi.doc_id = n.doc_id",
                    "pi.path = '%s'" % self.PROD_END)

        # What is the SFTP user account name?
        query.outer("query_term un", "un.doc_id = n.doc_id",
                    "un.path = '%s'" % self.USER_NAME)

        # Order the licensees by name, grouped by status.
        query.order("s.value", "n.value")

        # Collect and save the Partner objects.
        control.logger.debug("database query:\n%s", query)
        rows = query.execute(control.cursor)
        cols = [description[0] for description in control.cursor.description]
        rows = [dict(zip(cols, row)) for row in rows]
        self.licensees = [Partner(self, row) for row in rows]

    def table(self):
        """
        Assemble and return the object for the report's HTML table.
        """

        rows = [licensee.row() for licensee in self.licensees]
        headers = self.header_row()
        active = "Active Partners: %d" % self.prod_count
        test = "Test Partners: %d" % self.test_count
        caption = "%s - %s" % (active, test)
        caption_style = "font-size: 1.3em; font-weight: bold;"
        caption = Control.B.CAPTION(caption, style=caption_style)
        return Control.B.TABLE(caption, headers, *rows, style=Control.TSTYLE)

    def header_row(self):
        """
        Assemble an object for the row of table column headers.
        """

        return Control.B.TR(
            Control.th("CDR ID"),
            Control.th("Partner Name"),
            Control.th("Status"),
            Control.th("Test Started"),
            Control.th("Test Renewed"),
            Control.th("Test Removed"),
            Control.th("Prod Started"),
            Control.th("Prod Removed"),
            Control.th("Last Access")
        )


class Partner:
    """
    Object holding the information needed for a single row in the report.

    Properties:
        doc_id              primary key for the CDR Licensee document
        name                organization name for the data partner
        test_activation     date the test period started
        test_extension      date the test period was extended
        test_inactivation   date the test period concluded
        prod_activation     date the production period began
        prod_inactivation   date the production period ended
    """

    def __init__(self, partners, values):
        """
        Collect all of the properties for a single PDQ data partner.
        Because we've told our connection object to return dictionaries
        for the result set rows, and we have been careful to name the
        columns in the result set with the property names needed for
        our object, all we have to do is iterate through the dictionary's
        keys and values. Keep track of how many partners are in
        production, and how many are still in the test phase.
        """

        for name, value in values.items():
            setattr(self, name, value)
        if self.status.lower() == "production":
            Partners.prod_count += 1
        else:
            Partners.test_count += 1
        self.last_access = None
        if self.user_name:
            self.last_access = partners.last_access.get(self.user_name.lower())

    def row(self):
        """
        Assemble the object for the table row containing the values
        for this data partner.
        """

        return Control.B.TR(
            Control.td(str(self.doc_id)),
            Control.td(self.name),
            Control.td(self.status),
            Control.td(self.test_activation, white_space="nowrap"),
            Control.td(self.test_extension, white_space="nowrap"),
            Control.td(self.test_inactivation, white_space="nowrap"),
            Control.td(self.prod_activation, white_space="nowrap"),
            Control.td(self.prod_inactivation, white_space="nowrap"),
            Control.td(self.last_access, white_space="nowrap")
        )


if __name__ == "__main__":
    """
    Make it possible to run this task from the command line.
    You'll have to modify the PYTHONPATH environment variable
    to include the parent of this file's directory.
    """

    import argparse
    import logging
    fc = argparse.ArgumentDefaultsHelpFormatter
    desc = "Report on active licensees"
    parser = argparse.ArgumentParser(description=desc, formatter_class=fc)
    parser.add_argument("--mode", choices=Control.MODES, required=True,
                        help="controls who gets the report")
    parser.add_argument("--skip-email", action="store_true",
                        help="just write the report to the file system")
    parser.add_argument("--log-level", choices=("info", "debug", "error"),
                        default="info", help="verbosity of logging")
    parser.add_argument("--recip", help="optional email address for testing")
    args = parser.parse_args()
    opts = dict(format=cdr.Logging.FORMAT, level=args.log_level.upper())
    logging.basicConfig(**opts)
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    Control(opts, logging.getLogger()).run()
