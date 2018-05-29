"""
Logic for reports on the translation job queue
"""

import cdr
import cdrdb2 as cdrdb
import datetime

from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag

class ReportTask(CDRTask):
    """
    Implements subclass for managing scheduled translation job reports.
    """

    LOGNAME = "scheduled_translation_job_report"

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

    Instance properties:

    mode            Required report mode ("test" or "live").
    test            Convenience Boolean reflecting whether mode is 'test'.
    logger          Object for recording log information about the report.
    cursor          Object for submitting queries to the database.
    """

    def __init__(self, options, logger):
        """
        Save the logger object and extract and validate the settings:

        mode
            must be "test" or "live" (required); test mode restricts
            recipient list for report
        """

        self.logger = logger
        self.mode = options["mode"]
        self.test = self.mode == "test"
        self.title = "Translation Jobs Queue Report"
        self.cursor = cdrdb.connect("CdrGuest").cursor()

    def run(self):
        "Generate and email the report."
        self.logger.info("top of run, loading users")
        for user in self.load_users():
            user.send_report(self)
        self.logger.info("job completed")

    def load_users(self):
        users = []
        missing_email = set()
        fields = ("d.id", "d.title", "s.value_name", "u.name", "u.fullname",
                  "u.id", "u.email", "j.state_date")
        query = cdrdb.Query("summary_translation_job j", *fields)
        query.join("usr u", "u.id = j.assigned_to")
        query.join("document d", "d.id = j.english_id")
        query.join("summary_translation_state s", "s.value_id = j.state_id")
        query.where("s.value_name <> 'Translation Made Publishable'")
        query.order("u.id", "s.value_pos", "j.state_date", "d.title")
        rows = query.execute(self.cursor).fetchall()
        for doc_id, title, state, name, fullname, uid, email, date in rows:
            if email:
                if not users or uid != users[-1].uid:
                    user = User(uid, name, fullname, email)
                    users.append(user)
                user.add_job(state, date, doc_id, title)
            elif uid not in missing_email:
                self.logger.error("user %s has no email address" % name)
                missing_email.add(uid)
        return users

class User:
    """
    TEST            Template for identifying redirected message for test run
    SENDER          First argument to cdr.sendMail().
    CHARSET         Encoding used by cdr.sendMail().
    TSTYLE          CSS formatting rules for table elements.
    TO_STRING_OPTS  Options used for serializing HTML report object.
    B               HTML builder module imported at Control class scope.
    HTML            HTML module imported at Control class scope.
    """

    import lxml.html.builder as B
    import lxml.html as HTML
    TEST = "*** TEST MESSAGE *** LIVE MODE WOULD HAVE GONE TO %s"
    SENDER = "PDQ Operator <NCIPDQoperator@mail.nih.gov>"
    CHARSET = "iso-8859-1"
    TSTYLE = (
        "width: 95%",
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

    def __init__(self, uid, name, fullname, email):
        self.uid = uid
        self.name = name
        self.fullname = fullname
        self.email = email
        self.jobs = []
    def add_job(self, state, date, doc_id, title):
        self.jobs.append(self.Job(state, date, doc_id, title))
    def send_report(self, control):
        report = self.create_report(control.test)
        control.logger.debug("report\n%s", report)
        if control.test:
            group = "Test Translation Queue Recips"
            recips = CDRTask.get_group_email_addresses(group)
        else:
            recips = [self.email]
        if recips:
            subject = "[%s] %s" % (cdr.Tier().name, control.title)
            cdr.sendMail(self.SENDER, recips, subject, report, html=True)
            control.logger.info("sent %s", subject)
            control.logger.info("recips: %s", ", ".join(recips))
        else:
            control.logger.error("no email recipients for %s", group)
    def create_report(self, test=False):
        title = u"Translation Jobs for %s" % self.fullname
        style = "font-size: .9em; font-style: italic; font-family: Arial"
        body = self.B.BODY(
            self.B.H3(title, style="color: navy; font-family: Arial;"),
            self.B.P("Report date: %s" % datetime.date.today(), style=style),
        )
        if test:
            body.append(self.B.P(self.TEST % self.email))
        self.add_tables(body)
        report = self.B.HTML(
            self.B.HEAD(
                self.B.META(charset=self.CHARSET),
                self.B.TITLE(title),
            ),
            body
        )
        return self.serialize(report)
    def add_tables(self, body):
        style = "font-weight: bold; font-size: 1.2em; font-family: Arial"
        style += "; text-align: left;"
        caption = table = None
        for job in self.jobs:
            if table is None or job.state != caption:
                caption = job.state
                table = self.B.TABLE(
                    self.B.CAPTION(caption, style=style),
                    self.B.TR(
                        self.th("Date"),
                        self.th("CDR ID"),# width="10%"),
                        self.th("Title"),# width="90%"),
                    ),
                    style=self.TSTYLE
                )
                body.append(table)
            table.append(job.tr())

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

    class Job:
        def __init__(self, state, date, doc_id, title):
            self.state = state
            self.date = date
            self.doc_id = doc_id
            self.title = title
        def tr(self):
            return User.B.TR(
                User.td(str(self.date)[:10], white_space="nowrap"),
                User.td(str(self.doc_id)),
                User.td(self.title)
            )

def main():
    """
    Make it possible to run this task from the command line.
    You'll have to modify the PYTHONPATH environment variable
    to include the parent of this file's directory.
    """

    import argparse
    import logging
    fc = argparse.ArgumentDefaultsHelpFormatter
    desc = "Report on CDR translation jobs"
    parser = argparse.ArgumentParser(description=desc, formatter_class=fc)
    parser.add_argument("--mode", choices=("test", "live"), required=True,
                        help="controls who gets the report")
    parser.add_argument("--log-level", choices=("info", "debug", "error"),
                        default="info", help="verbosity of logging")
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    logging.basicConfig(format=cdr.Logging.FORMAT, level=args.log_level.upper())
    Control(opts, logging.getLogger()).run()

if __name__ == "__main__":
    main()
