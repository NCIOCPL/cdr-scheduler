"""
Logic for reports on the translation job queues
"""

import cdr
import cdrdb2 as cdrdb
import datetime

from .cdr_task_base import CDRTask
from .task_property_bag import TaskPropertyBag

class ReportTask(CDRTask):
    """
    Subclass for managing scheduled summary translation job reports.
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

class ReportTools:
    """
    Common functionality for building/sending the reports

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
    TEST = "*** TEST MESSAGE *** LIVE MODE WOULD HAVE GONE TO {}"
    SENDER = "PDQ Operator <NCIPDQoperator@mail.nih.gov>"
    CHARSET = "utf-8"
    TSTYLE = (
        "width: 95%",
        "border: 1px solid #999",
        "border-collapse: collapse",
        "margin-top: 30px"
    )
    TSTYLE = "; ".join(TSTYLE)
    TO_STRING_OPTS = {
        "pretty_print": True,
        "encoding": "unicode",
        "doctype": "<!DOCTYPE html>"
    }

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


class Control(ReportTools):
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.

    Instance properties:

    mode            Required report mode ("test" or "live").
    test            Convenience Boolean reflecting whether mode is 'test'.
    logger          Object for recording log information about the report.
    cursor          Object for submitting queries to the database.
    """

    DOCTYPES = "Summary", "Media", "Glossary"
    SCHEDULES = "weekly", "nightly"

    def __init__(self, options, logger):
        """
        Save the logger object and extract and validate the settings:

        mode
            must be "test" or "live" (required); test mode restricts
            recipient list for report
        """

        self.logger = logger
        self.mode = options["mode"]
        self.doctype = options.get("doctype", "Summary")
        self.schedule = options.get("schedule", "nightly")
        message = "Unsupported document type {!r}".format(self.doctype)
        assert self.doctype in self.DOCTYPES, message
        self.test = self.mode == "test"
        title = "Translation Jobs Queue Report"
        if self.schedule == "weekly":
            title = "Documents Ready For Translation"
        self.title = "{} {}".format(self.doctype, title)
        self.cursor = cdrdb.connect("CdrGuest").cursor()

    def run(self):
        "Generate and email the report."
        self.logger.info("*** top of %s %s run", self.schedule, self.doctype)
        if self.schedule == "nightly":
            self.logger.info("loading users")
            for user in self.load_users():
                user.send_report(self)
        else:
            jobs = self.load_jobs()
            self.send_report(jobs)
        self.logger.info("%s, %s job completed", self.schedule, self.doctype)

    def load_users(self):
        """
        Collect information on users with active translation jobs

        Work is handed off to a specialized method for the job's
        document type.

        Return:
          sequence of `User` objects
        """

        users = []
        missing_email = set()
        rows = getattr(self, "load_{}_users".format(self.doctype.lower()))()
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

    def load_summary_users(self):
        """
        Collect information on users with active Summary translation jobs

        Return:
          sequence of `User` objects
        """

        fields = ("d.id", "d.title", "s.value_name", "u.name", "u.fullname",
                  "u.id", "u.email", "j.state_date")
        query = cdrdb.Query("summary_translation_job j", *fields)
        query.join("usr u", "u.id = j.assigned_to")
        query.join("document d", "d.id = j.english_id")
        query.join("summary_translation_state s", "s.value_id = j.state_id")
        query.where("s.value_name <> 'Translation Made Publishable'")
        query.order("u.id", "s.value_pos", "j.state_date", "d.title")
        return query.execute(self.cursor).fetchall()

    def load_media_users(self):
        """
        Collect information on users with active Media translation jobs

        Return:
          sequence of `User` objects
        """

        fields = ("d.id", "d.title", "s.value_name", "u.name", "u.fullname",
                  "u.id", "u.email", "j.state_date")
        query = cdrdb.Query("media_translation_job j", *fields)
        query.join("usr u", "u.id = j.assigned_to")
        query.join("document d", "d.id = j.english_id")
        query.join("media_translation_state s", "s.value_id = j.state_id")
        query.order("u.id", "s.value_pos", "j.state_date", "d.title")
        return query.execute(self.cursor).fetchall()

    def load_glossary_users(self):
        """
        Collect information on users with active Glossary translation jobs

        Documents can be of type GlossaryTermName or GlossaryTermConcept.

        Return:
          sequence of `User` objects
        """

        fields = ("d.id", "t.name", "s.value_name", "u.name", "u.fullname",
                  "u.id", "s.value_pos", "u.email", "j.state_date")
        query = cdrdb.Query("glossary_translation_job j", *fields)
        query.join("usr u", "u.id = j.assigned_to")
        query.join("document d", "d.id = j.doc_id")
        query.join("doc_type t", "t.id = d.doc_type")
        query.join("glossary_translation_state s", "s.value_id = j.state_id")
        rows = query.execute(self.cursor).fetchall()
        users = {}
        for doc_id, doc_type, state, name, full, uid, pos, email, date in rows:
            title = self.get_glossary_title(doc_id, doc_type)
            key = uid, pos, str(date)[:10], title
            values = doc_id, title, state, name, full, uid, email, date
            users[key] = values
        return [users[key] for key in sorted(users)]

    def get_glossary_title(self, doc_id, doc_type):
        """
        Fetch or construct title for Glossary document

        For GlossaryTermConcept documents we construct a title in
        the form: "GTC for [title of first GTN document]

        Return:
          string representing document's title
        """

        if doc_type == "GlossaryTermConcept":
            path = "/GlossaryTermName/GlossaryTermConcept/@cdr:ref"
            query = cdrdb.Query("document d", "d.title").limit(1)
            query.join("query_term q", "q.doc_id = d.id")
            query.where(query.Condition("q.path", path))
            query.where(query.Condition("q.int_val", doc_id))
            query.order("d.title")
            row = query.execute(self.cursor).fetchone()
            if row:
                return "GTC for {}".format(row[0])
            return "GTC CDR{:d}".format(doc_id)
        query = cdrdb.Query("document", "title")
        query.where(query.Condition("id", doc_id))
        return query.execute(self.cursor).fetchone()[0]

    def load_jobs(self):
        """
        Find the jobs which have the state "Ready for Translation"

        Hands off much of the work to specialized methods for
        different document types. Summaries currently not
        supported.

        Return:
          Sequence of tuples of values
        """

        return getattr(self, "load_{}_jobs".format(self.doctype.lower()))()

    def load_media_jobs(self):
        """
        Find Media jobs which have the state "Ready for Translation"

        Return:
          Sequence of tuples of values
        """

        fields = ("d.title", "d.id", "u.fullname", "j.state_date")
        query = cdrdb.Query("media_translation_job j", *fields)
        query.join("usr u", "u.id = j.assigned_to")
        query.join("document d", "d.id = j.english_id")
        query.join("media_translation_state s", "s.value_id = j.state_id")
        query.where("s.value_name = 'Ready for Translation'")
        query.order("d.title")
        return query.execute(self.cursor).fetchall()

    def load_glossary_jobs(self):
        """
        Find Glossary jobs which have the state "Ready for Translation"

        Complicated by the fact that we have to construct a title for
        GlossaryTermConcept documents using the title of one of its
        GlossaryTermName documents.

        Return:
          Sequence of tuples of values
        """

        fields = ("d.id", "t.name", "u.fullname", "j.state_date")
        query = cdrdb.Query("glossary_translation_job j", *fields)
        query.join("usr u", "u.id = j.assigned_to")
        query.join("document d", "d.id = j.doc_id")
        query.join("doc_type t", "t.id = d.doc_type")
        query.join("glossary_translation_state s", "s.value_id = j.state_id")
        query.where("s.value_name = 'Ready for Translation'")
        jobs = {}
        rows = query.execute(self.cursor).fetchall()
        for doc_id, doc_type, name, date in rows:
            title = self.get_glossary_title(doc_id, doc_type)
            jobs[(title.lower(), doc_id)] = (title, doc_id, name, date)
        return [jobs[key] for key in sorted(jobs)]

    def send_report(self, jobs):
        """
        Send weekly report of new translation jobs to lead translator

        Pass:
          Sequence of tuples of values
        """

        report = self.create_report(jobs)
        self.logger.debug("report\n%s", report)
        group = "Spanish Translation Leads"
        if self.test:
            group = "Test Translation Queue Recips"
        recips = CDRTask.get_group_email_addresses(group)
        if recips:
            subject = "[%s] %s" % (cdr.Tier().name, self.title)
            cdr.sendMailMime(self.SENDER, recips, subject, report, "html")
            self.logger.info("sent %s", subject)
            self.logger.info("recips: %s", ", ".join(recips))
        else:
            self.logger.error("no email recipients for %s", group)

    def create_report(self, jobs):
        title = "New {} Translation Jobs".format(self.doctype)
        style = "font-size: .9em; font-style: italic; font-family: Arial"
        today = datetime.date.today()
        report = self.B.HTML(
            self.B.HEAD(
                self.B.META(charset=self.CHARSET),
                self.B.TITLE(title),
            ),
            self.B.BODY(
                self.B.H3(title, style="color: navy; font-family: Arial;"),
                self.B.P("Report date: {}".format(today), style=style),
                self.make_table(jobs)
            )
        )
        return self.serialize(report)

    def make_table(self, jobs):
        style = "font-weight: bold; font-size: 1.2em; font-family: Arial"
        style += "; text-align: left;"
        table = self.B.TABLE(
            self.B.CAPTION("Ready For Translation", style=style),
            self.B.TR(
                self.th("Title"),
                self.th("CDR ID"),
                self.th("Translator"),
                self.th("Date"),
            ),
            style=self.TSTYLE
        )
        for title, doc_id, user, date in jobs:
            tr = self.B.TR(
                self.td(title),
                self.td(str(doc_id)),
                self.td(user),
                self.td(str(date)[:10], white_space="nowrap")
            )
            table.append(tr)
        return table


class User(ReportTools):
    """
    Translator who will receive a nightly jobs report
    """

    def __init__(self, uid, name, fullname, email):
        self.uid = uid
        self.name = name
        self.fullname = fullname
        self.email = email
        self.jobs = []

    def add_job(self, state, date, doc_id, title):
        self.jobs.append(self.Job(state, date, doc_id, title))

    def send_report(self, control):
        report = self.create_report(control)
        control.logger.debug("report\n%s", report)
        if control.test:
            group = "Test Translation Queue Recips"
            recips = CDRTask.get_group_email_addresses(group)
        else:
            recips = [self.email]
        if recips:
            subject = "[%s] %s" % (cdr.Tier().name, control.title)
            cdr.sendMailMime(self.SENDER, recips, subject, report, "html")

            control.logger.info("sent %s", subject)
            control.logger.info("recips: %s", ", ".join(recips))
        else:
            control.logger.error("no email recipients for %s", group)

    def create_report(self, control):
        args = control.doctype, self.fullname
        title = "{} Translation Jobs for {}".format(*args)
        style = "font-size: .9em; font-style: italic; font-family: Arial"
        body = self.B.BODY(
            self.B.H3(title, style="color: navy; font-family: Arial;"),
            self.B.P("Report date: %s" % datetime.date.today(), style=style),
        )
        if control.test:
            body.append(self.B.P(self.TEST.format(self.email)))
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
                        self.th("CDR ID"),
                        self.th("Title"),
                    ),
                    style=self.TSTYLE
                )
                body.append(table)
            table.append(job.tr())


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
    parser.add_argument("--doctype", choices=Control.DOCTYPES)
    parser.add_argument("--schedule", choices=Control.SCHEDULES)
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    logging.basicConfig(format=cdr.Logging.FORMAT, level=args.log_level.upper())
    Control(opts, logging.getLogger()).run()

if __name__ == "__main__":
    main()
