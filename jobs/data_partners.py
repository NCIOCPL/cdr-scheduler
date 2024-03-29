"""
Scheduled tasks related to PQD data partners.

When this job is run on a non-production tier, the email messages
will be sent to the members of the "Test Publishing Notification"
CDR group instead of the outside partners' email addresses.
"""

from .base_job import Job
import datetime
import os
import time
import lxml.etree as etree
import lxml.html as html
import lxml.html.builder as B
import requests
import cdr
from cdrapi.users import Session
from cdrapi import db
from cdrapi.docs import Doc
from html import escape as html_escape


class Notify(Job):
    """Communication with PDQ data partners about fresh PDQ data.

    Task for notifying the PDQ data partners when the weekly refresh
    of PDQ data has been successfully completed.

    Attributes:
      conn - reference to database connection object
      cursor - reference to database cursor for connection
      logger - object for recording what we do
      test - flag indicating whether this is a test run
      ops - list of email addresses for notifying NCI staff
      log_messages - flag indicating whether to log full email messages
    """

    LOGNAME = "data-partner-notification"
    SENDER = "NCI PDQ\u00ae Operator <NCIPDQoperator@mail.nih.gov>"
    MAX_TRIES = 5
    DELAY = 5
    MODES = "test", "live"
    SUPPORTED_PARAMETERS = {"job", "superlog", "recip", "mode"}

    test_recips = None

    @property
    def conn(self):
        if not hasattr(self, "_conn"):
            self._conn = db.connect()
        return self._conn

    @property
    def contacts(self):
        """List of contacts for partners which have not already expired."""

        if not hasattr(self, "_contacts"):
            url = f"https://{cdr.APPC}/cgi-bin/cdr/get-pdq-contacts.py"
            self.logger.info("fetching contacts from %r", url)
            root = etree.fromstring(requests.get(url).content)
            contacts = [Contact(self, n) for n in root.findall("contact")]
            self.logger.info("%d contacts fetched", len(contacts))
            self._contacts = contacts
        return self._contacts

    @property
    def cursor(self):
        if not hasattr(self, "_cursor"):
            self._cursor = self.conn.cursor()
        return self._cursor

    @property
    def job(self):
        if not hasattr(self, "_job"):
            args = self.cursor, self.logger, self.opts.get("job"), self.test
            self._job = self.ExportJob(*args)
        return self._job

    @property
    def log_messages(self):
        if not hasattr(self, "_log_messages"):
            self._log_messages = True if self.opts.get("superlog") else False
        return self._log_messages

    @property
    def ops(self):
        """Operator accounts."""

        if not hasattr(self, "_ops"):
            group = "PDQ Partner Notification"
            self._ops = self.get_group_email_addresses(group)
        return self._ops

    @property
    def recip(self):
        """Optional test recipient for all email messages."""

        if not hasattr(self, "_recip"):
            self._recip = self.opts.get("recip")
        return self._recip

    @property
    def test(self):
        if not hasattr(self, "_test"):
            mode = self.opts.get("mode")
            if mode not in self.MODES:
                self.logger.error("invalid mode %r", mode)
                raise Exception("invalid or missing mode")
            self._test = mode != "live"
        return self._test

    @property
    def session(self):
        if not hasattr(self, "_session"):
            opts = dict(password=cdr.getpw("pdqcontent"))
            self._session = Session.create_session("pdqcontent", **opts)
        return self._session

    def run(self):
        """
        Send notifications to the active PDQ data partners.

        Most partners will just get the notification that fresh PDQ data
        is ready to be retrieved. Test accounts which are nearing their
        expiration dates will also get warnings about the upcoming
        expiration. Test accounts which have passed their expiration dates
        will only get a notification that their accounts have expired.
        The database table for the notification will be updated to
        reflect dates of notification. Expiration of a test account
        will result in a modification of the Licensee document.

        The actual disabling of login access to the sFTP server is a
        separate step handled by CBIIT at our request.
        """

        for contact in self.contacts:
            contact.process()
        self.report()
        self.logger.info("notification job complete")

    def report(self):
        """
        Send a summary report of processing to the ops team.
        """

        if self.recip:
            recips = [self.recip]
        else:
            recips = self.ops
        message = open(self.job.report_path, encoding="utf-8").read() + "</ul>"
        subject = "Summary Report on Notification: %s" % self.job.subject
        self.send(recips, subject, message)
        self.logger.info("sent summary report to %r", recips)

    def send(self, recips, subject, message):
        """
        Send an email message, trying multiple times before giving up.

        We pause between attempts in order to avoid flooding the NIH
        mail server with too many requests at the same time.

        Note that the `recips` argument will always be a sequence
        of addresses when we are sending internal messages reporting
        processing to the operators, and will always be a single
        string when sending the notification to an individual data
        partner contact. This is why the checks which prevent sending
        out email messages to outside data partners from non-production
        tiers works correctly.

        Pass:
            recips - string (for partner contact) or list of strings (for ops)
            subject - string for message's subject header
            message - string for message body

        Raise:
            exception propagated by sendmail if too many attempts fail
        """

        if isinstance(recips, str):
            if not cdr.isProdHost() or self.test:
                extra = "In live prod mode, would have gone to %r" % recips
                if Notify.test_recips is None:
                    if self.recip:
                        Notify.test_recips = [self.recip]
                    else:
                        group = "Test Publishing Notification"
                        test_recips = self.get_group_email_addresses(group)
                        Notify.test_recips = test_recips
                recips = Notify.test_recips
                self.logger.info("using recips: %r", recips)
                message = "<h3>%s</h3>\n%s" % (html_escape(extra), message)
            else:
                recips = [recips]
        tries, delay = 0, self.DELAY
        while True:
            self.logger.debug("top of send(%r)", recips)
            try:
                opts = dict(subject=subject, body=message, subtype="html")
                msg = cdr.EmailMessage(self.SENDER, recips, **opts)
                msg.send()
                msg = str(msg)
                if self.log_messages:
                    self.logger.debug(msg)
                return msg
            except Exception:
                self.logger.exception("failure sending to %r", recips)
                tries += 1
                if tries >= self.MAX_TRIES:
                    self.logger.error("bailing after %d tries", self.MAX_TRIES)
                    raise
                self.logger.debug("pausing %s seconds", delay)
                time.sleep(delay)
                delay += self.DELAY

    class ExportJob:
        """
        Information about the export job for which we send notifications.

        Instance attributes:
            id - integer for the job's primary key
            started - when the job began processing
            directory - path to the location of the job's data partner output
            year - integer for the year in which the jobs ISO week falls
            week - integer for the ISO week when the job started
            day - day of the week when the job started (1=Monday)
            message - string for the notification sent to the data partners
            subject - string for the subject line of the notification
            report_path - location of the file accumulating information
                          to be mailed to the operator(s)
        """

        def __init__(self, cursor, logger, job_id=None, test=False):
            """
            Collect the attributes of the job for which we send notifications.
            """

            self.logger = logger
            query = db.Query("pub_proc", "id", "started")
            if job_id:
                query.where(query.Condition("id", job_id))
            else:
                query.where("pub_subset = 'Export'")
                query.where("status = 'Success'")
                query.order("id DESC")
                query.limit(1)
            row = query.execute(cursor).fetchone()
            logger.info("notifications for job %d started %s", row[0], row[1])
            self.id, self.started = row
            self.directory = f"{cdr.BASEDIR}/Output/LicenseeDocs/Job{self.id}"
            self.year, self.week, self.day = self.started.isocalendar()
            self.message = self.load_notification_message(test)
            self.subject = self.create_subject(test)
            self.report_path = self.get_report_path(test)

        def get_report_path(self, test):
            """
            Get the location of the file collecting information for ops staff.

            If the file doesn't yet exist (which will be true most of the
            time), create it and write the first part of the message to
            be sent to the staff. The file will only already exist if a
            previous attempt to send out notifications failed part-way
            through, and we're resuming to notify the data partners who
            didn't get notified by the earlier run.
            """

            path = "%s/report" % self.directory
            if not os.path.isfile(path):
                self.logger.debug("creating %r", path)
                style = "text-align: center; background: #365f91; color: white"
                top = "Test Vendor Memo" if test else "Vendor Memo"
                header = "Notification sent to the following vendors"
                with open(path, "w") as fp:
                    fp.write('<h2 style="%s"> %s </h2>\n' % (style, top))
                    fp.write("%s\n" % self.message)
                    fp.write('<h2 style="%s">End Vendor Memo</h2>\n' % style)
                    fp.write("<h3>%s</h3>\n<ul>\n" % header)
            return path

        def date_and_week(self):
            """
            Format string Volker wants for date + week in the subject lines.
            """

            return "%s (Week %s)" % (str(self.started)[:10], self.week)

        def create_subject(self, test):
            """
            Build the subject line for the notification message.
            """

            prefix = "Test" if test else "PDQ"
            return "%s XML Data for %s" % (prefix, self.date_and_week())

        def load_notification_message(self, test=False):
            """
            Assemble the body for the message to be sent to the data partners.

            The top portion of the message is pulled from the ctl table,
            and the rest contains statistical information about what
            changed since last week's publishing job.
            """

            name = "%s-notification" % ("test" if test else "data-partner")
            message = cdr.getControlValue("Publishing", name)
            return "%s\n%s\n" % (message, self.format_stats())

        def format_stats(self):
            """
            Create a formatted report summarizing deltas from last week's job.

            Values for the report are drawn from the colon-delimited records
            in the YYYYMM.changes file written to the job's vendor output
            directory.
            """

            table = B.TABLE(style="width: 400px;")
            values = self.week, self.year
            caption = "Changed Documents for Week %s, %s" % values
            table.append(B.CAPTION(caption, style="border-bottom: solid 1px;"))
            header_row = B.TR()
            for header in ("Document Type", "Added", "Modified", "Removed"):
                header_row.append(B.TH(header, style="text-align: right;"))
            table.append(header_row)
            path = "%s/%s%02d.changes" % (self.directory, self.year, self.week)
            for line in sorted(open(path)):
                doctype, action, count = line.strip().split(":")
                if action == "added":
                    name = doctype.split(".")[0]
                    row = B.TR(B.TD(name, style="text-align: right;"))
                    table.append(row)
                row.append(B.TD(count, style="text-align: right;"))
            return html.tostring(table, encoding="unicode")


class Contact:
    """
    Information about a contact for one of the PDQ data partners.

    Test accounts are temporary. We advertise such accounts as having a
    three-month duration, though we actually give them 100 days before
    they expire. We warn them approximately 20 days in advance when the
    expiration is imminent. Other account types do not expire.

    Instance attributes:
        control - reference to the top-level object managing the job
        logger - object for logging processing activity and errors
        job - reference to object for the job we're sending notifications for
        email - address for the notification
        person - name of the person with whom we are corresponding
        org - name of the organization for the account
        org_id - primary key for the account's organization
        type - code for the type of account (e.g., A (Active))
        type_string - see Contact.TYPE_STRINGS
        activated - when the account was first activated
        renewed - when the account was renewed (optional)
        notified - when the account last received notification
        display - PERSON-NAME at ORG-NAME <EMAIL-ADDRESS>
        expired - flag indicating whether account needs to be deactivated
        expiring - date in the near future when the account will expire
                   (when applicable)
    """

    TODAY = datetime.date.today()
    EXPIRATION_THRESHOLD = str(TODAY - datetime.timedelta(100))
    WARNING_THRESHOLD = str(TODAY - datetime.timedelta(80))
    TYPE_STRINGS = {"T": "Test", "A": "Active", "S": "Special"}
    DELAY = 3
    TABLE = "data_partner_notification"
    COLS = "email_addr, notif_date"
    INSERT = "INSERT INTO {} ({}) VALUES (?, GETDATE())".format(TABLE, COLS)

    def __init__(self, control, node):
        """
        Extract the partner/contact information from the XML node.

        Also determine whether the partner's account (if it is
        a temporary, test account) has expired (or is about to
        expire).
        """

        self.control = control
        self.logger = control.logger
        self.job = control.job
        self.contact_id = node.get("pid")
        self.email = cdr.get_text(node.find("email_address"))
        self.person = cdr.get_text(node.find("person_name"))
        self.org = cdr.get_text(node.find("org_name"))
        self.type = cdr.get_text(node.find("org_status"))
        self.type_string = "%s Vendor" % self.TYPE_STRINGS[self.type]
        self.activated = cdr.get_text(node.find("activation_date"))
        self.renewed = cdr.get_text(node.find("renewal_date"))
        self.notified = cdr.get_text(node.find("notified_date"))
        self.org_id = cdr.get_text(node.find("org_id"))
        self.display = "%s at %s <%s>" % (self.person, self.org, self.email)
        self.expired = self.expiring = False
        if self.type == "T":
            start = self.renewed or self.activated
            if start < self.EXPIRATION_THRESHOLD:
                self.expired = True
            elif start < self.WARNING_THRESHOLD:
                start = datetime.datetime.strptime(start[:10], "%Y-%m-%d")
                self.expiring = start + datetime.timedelta(100)
        self.logger.debug(self.display)

    def process(self):
        """
        Send the email message(s) appropriate for this account.

        In the usual case, a single email message is sent informing
        the contact person for the account that the latest set of
        PDQ data is ready for retrieval. If the account is an
        expired test account, that notification message is replaced
        by a notification that the account is no longer active.
        If the account is a test account, and is close to expiration,
        we send the notification for the fresh data, but we also
        send a separate warning message about the impending deactivation.

        In the event that this run is a resumption of a previous job
        which failed partway through, we skip over the notifications
        we've already sent.

        In all other cases, after sending out the appropriate email
        messages, we pause for a few seconds, to prevent the NIH email
        server from blocking our access for flooding it with too many
        requests at the same time.
        """

        if self.notified >= str(self.job.started):
            self.logger.info("%s notified %s", self.display, self.notified)
            return
        if self.expired:
            self.disable()
        else:
            self.notify()
            if self.expiring:
                self.warn()
        time.sleep(self.DELAY)

    def notify(self):
        """
        Tell the data partner we have fresh data to be retrieved.

        We also append a line to the summary report to be sent to
        the operators, and we update the contact's database record
        to record that the notification has been sent.
        """

        summary = "%s: %s" % (self.type_string, self.display)
        if self.expiring:
            summary += " (warning notice sent)"
        self.logger.info("notifying %s", self.display)
        self.report(summary)
        self.send(self.job.subject, self.job.message)
        if not self.control.test:
            email = self.email.lower().strip()
            self.control.cursor.execute(self.INSERT, email)
            self.control.conn.commit()

    def warn(self):
        """
        Send an email message saying that the account will be deactivated soon.

        Also, send a separate message immediately to the operators, containing
        the warning message we just sent to the data partner.

        N.B.: If we're resuming after a partially-failed run, it is possible
        that multiple warning messages will be sent to the same partner,
        because (unlike normal notification) there is no mechanism for
        recording when the last warning was sent.
        """

        self.logger.info("warning %s of pending expiration", self.display)
        subject = "Warning notice: NCI PDQ Test Account for %s" % self.org
        subject = "%s, %s" % (subject, self.job.date_and_week())
        template = cdr.getControlValue("Publishing", "test-partner-warning")
        message = template.replace("@@EXPIRING@@", str(self.expiring)[:10])
        self.send(subject, message)
        self.notify_ops(subject, message)

    def disable(self):
        """
        Disable the account, and send a notice of expiration.

        Also, add a line to the summary report to be sent to the operators,
        and send a separate message immediately to the operators, containing
        the termination message we just sent to the data partner.

        N.B.: If processing fails between the step to send the expiration
        notice and the actual expiration itself, the partner will get a
        second notice when we correct the cause of the failure and resume
        processing. Very unlikely to happen.
        """

        self.logger.info("disabling %s", self.display)
        self.report("Disabled test account for %s" % self.display)
        subject = "Expiration notice: NCI PDQ Test Account for %s" % self.org
        subject = "%s, %s" % (subject, self.job.date_and_week())
        message = cdr.getControlValue("Publishing", "test-partner-disabled")
        self.send(subject, message)
        self.expire()
        self.notify_ops(subject, message)

    def expire(self):
        """
        Update data partner's CDR document to reflect deactivation.

        Note:
           The deactivation status values have changed from
             Test-inactive       --> Test-expired
             Production-inactive --> Production-terminated
           The old status values '%inactive' will now reflect partners
           who aren't updating the data but haven't officially been terminated.
           The corresponding status date elements, however, have not
           been modified.
             TestInactivationDate   reflects Test-expired status
             ProductionInactivation reflects Production-terminated status
        """

        doc = Doc(self.control.session, id=self.org_id)
        root = doc.root
        node = root.find("LicenseeInformation/LicenseeStatus")
        if node.text == "Test-expired":
            self.logger.warning("CDR%s already deactivated", self.org_id)
            return
        self.logger.info("Marking CDR%s as expired", self.org_id)
        node.text = "Test-expired"
        today = str(datetime.date.today())
        node = root.find("DateLastModified")
        if node is None:
            node = etree.SubElement(root, "DateLastModified")
        node.text = today
        node = root.find("LicenseeInformation/LicenseeStatusDates")
        if node is None:
            message = "CDR%s missing LicenseeStatusDates"
            self.logger.warning(message, self.org_id)
        else:
            child = node.find("TestInactivation")
            if child is None:
                child = etree.Element("TestInactivation")
                sibling = node.find("TestActivation")
                if sibling is not None:
                    sibling.addnext(child)
                else:
                    node.insert(0, child)
            if child.text is None or not child.text.strip():
                child.text = today
        if not self.control.test:
            comment = "Marking account as expired"
            doc.check_out(force=True, comment=comment)
            opts = dict(
                version=True,
                reason=comment,
                comment=comment,
                unlock=True,
            )
            doc.save(**opts)

    def send(self, subject, message):
        """
        Send an email message to the data partner's contact.
        """

        recip = "%s <%s>" % (self.person, self.email)
        self.control.send(recip, subject, message)

    def report(self, line):
        """
        Append a line to the summary report file for the operators.
        """

        with open(self.control.job.report_path, "a", encoding="utf-8") as fp:
            fp.write("<li>%s</li>\n" % html_escape(line))

    def notify_ops(self, subject, message):
        """
        Send an copy of a warning or expiration message to the operators.
        """

        contact = html_escape(self.display)
        lead = "The following message was sent to %s" % contact
        message = "<p><i>%s</i></p>%s" % (lead, message)
        self.control.send(self.control.ops, subject, message)
        self.logger.debug("copied ops on %r", subject)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    choices = "test", "live"
    parser.add_argument("--mode", choices=choices, required=True)
    parser.add_argument("--job", type=int)
    parser.add_argument("--superlog", action="store_true")
    parser.add_argument("--recip")
    opts = vars(parser.parse_args())
    Notify(None, "Notification Test", **opts).run()
