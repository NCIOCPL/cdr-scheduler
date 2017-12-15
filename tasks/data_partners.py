"""
Scheduled tasks related to PQD data partners.

There are basically two paths for testing this task without sending
email messages to any real outside partners.

The first approach is to set the `mode` flag to "test" and populate
the data partner database tables with partner records whose `product`
is "TEST" and whose contact email addresses belong to internal
staff (typically developers or testers). (Be careful not to be
confused by the overloaded word "TEST" in the partner records.
There is also a "TEST" status for outside partners who are trying
the system out. In hindsight, perhaps "PROVISIONAL" would have
been a better name for that status so it wouldn't be so easy to
mix up with the "TEST" product we're using here.) The upper tiers
often have no partner records with a "TEST" status, which means
that when the DEV (or QA) tier is refreshed from PROD, such records
will have to be re-created.

The other way to test is to run the task on a non-production tier,
with the `mode` flag set to "live"; this will cause the email messages
to be sent to the members of the "Test Publishing Notification"
CDR group instead of the outside partners' email addresses.
"""

import cgi
import datetime
import logging
import os
import time
import lxml.etree as etree
import lxml.html as html
import lxml.html.builder as B
import requests
import cdr
import cdrdb2 as cdrdb
import settings
from cdrutil import sendMail
from cdr_task_base import CDRTask
from core.exceptions import TaskException
from task_property_bag import TaskPropertyBag

class Notify(CDRTask):
    """
    Task for notifying the PDQ data partners when the weekly refresh
    of PDQ data has been successfully completed.
    """

    LOGNAME = "data-partner-notification"
    SENDER = u"NCI PDQ\u00ae Operator <NCIPDQoperator@mail.nih.gov>"
    MAX_TRIES = 5
    DELAY = 5
    MODES = "test", "live"

    def __init__(self, parms, data):
        """
        Create a logger, connect to the DB, and check the options.

        mode
            must be "test" or "live" (required); test mode restricts
            recipient list for report
        log-level
            optional logging level; defaults to "info"
        superlog
            if true, log full email messages (only effective if level
            is set to "debug")

        Attributes:
          logger - object for recording what we do
          test - flag indicating whether this is a test run
          ops - list of email addresses for notifying NCI staff
          log_messages - flag indicating whether to log full email messages
        """

        CDRTask.__init__(self, parms, data)
        self.log_messages = parms.get("superlog") and True or False
        mode = parms.get("mode")
        if mode not in self.MODES:
            self.logger.error("invalid mode %r", mode)
            raise Exception("invalid or missing mode")
        self.test = parms.get("mode") != "live"
        self.job = self.Job(self.logger, parms.get("job"), self.test)
        self.ops = self.get_group_email_addresses("PDQ Partner Notification")

    def Perform(self):
        """
        Send notifications to the active PDQ data partners.

        Most partners will just get the notification that fresh PDQ data
        is ready to be retrieved. Test accounts which are nearing their
        expiration dates will also get warnings about the upcoming
        expiration. Test accounts which have passed their expiration dates
        will only get a notification that their accounts have expired.
        The database table for the data partners will be updated to
        reflect dates of notification and expiration.

        The actual disabling of login access to the sFTP server is a
        separate step handled by CBIIT at our request.
        """

        for partner in Partner.get_partners(self):
            partner.process()
        self.report()
        self.logger.info("notification job complete")
        return TaskPropertyBag()

    def report(self):
        """
        Send a summary report of processing to the ops team.
        """

        message = open(self.job.report_path).read() + "</ul>"
        subject = "Summary Report on Notification: %s" % self.job.subject
        self.send(self.ops, subject, message)
        self.logger.info("sent summary report to %r", self.ops)

    def send(self, recips, subject, message):
        """
        Send an email message, trying multiple times before giving up.

        We pause between attempts in order to avoid flooding the NIH
        mail server with too many requests at the same time.

        Note that the `recips` argument will always be a sequence
        of addresses when we are sending internal messages reporting
        processing to the operators, and will always be a single
        string when sending the notification to an individual data
        partner. This is why the checks which prevent sending out
        email messages to outside data partners from non-production
        tiers works correctly (note also that if the `test` property
        is set, the email addresses will come from partner records
        which should have internal email addresses; see the notes
        at the top of this file).

        Pass:
            recips - string (for data partner) or list of strings (for ops)
            subject - string for message's subject header
            message - string for message body

        Raise:
            exception propagated by sendmail if too many attempts fail
        """

        if isinstance(recips, basestring):
            if not cdr.isProdHost() and not self.test:
                extra = u"In live prod mode, would have gone to %r" % recips
                group = "Test Publishing Notification"
                recips = self.get_group_email_addresses(group)
                self.logger.info("using recips: %r", recips)
                message = u"<h3>%s</h3>\n%s" % (cgi.escape(extra), message)
            else:
                recips = [recips]
        tries, delay = 0, self.DELAY
        while True:
            self.logger.debug("top of send(%r)", recips)
            try:
                msg = sendMail(self.SENDER, recips, subject, message, True)
                if self.log_messages:
                    self.logger.debug(msg)
                return msg
            except Exception as e:
                self.logger.exception("failure sending to %r", recips)
                tries += 1
                if tries >= self.MAX_TRIES:
                    self.logger.error("bailing after %d tries", self.MAX_TRIES)
                    raise
                self.logger.debug("pausing %s seconds", delay)
                time.sleep(delay)
                delay += self.DELAY

    class Job:
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

        def __init__(self, logger, job_id=None, test=False):
            """
            Collect the attributes of the job for which we send notifications.
            """

            self.logger = logger
            query = cdrdb.Query("pub_proc", "id", "started")
            if job_id:
                query.where(query.Condition("id", job_id))
            else:
                query.where("pub_subset = 'Export'")
                query.where("status = 'Success'")
                query.order("id DESC")
                query.limit(1)
            row = query.execute().fetchone()
            logger.info("notifications for job %d started %s", row[0], row[1])
            self.id, self.started = row
            values = cdr.BASEDIR, self.id
            self.directory = "%s/Output/LicenseeDocs/Job%d" % values
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
                product = "TEST" if test else "CDR"
                top = " Memo submitted to %s Vendors " % product
                header = "Notification sent to the following vendors"
                with open(path, "w") as fp:
                    fp.write('<h2 style="%s">%s</h2>\n' % (style, top))
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

            The values for the report are drawn from the colon-delimited records
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
            return html.tostring(table)

class Partner:
    """
    Information about a single PDQ data partner.

    Test accounts are temporary. We advertise such accounts as having a
    three-month duration, though we actually give them 100 days before
    they expire. We warn them approximately 20 days in advance when the
    expiration is imminent. Other account types do not expire.

    Instance attributes:
        control - reference to the top-level object managing the job
        logger - object for logging processing activity and errors
        job - reference to object for the job we're sending notifications for
        contact_id - primary key for the individual to whom we send email
        email - address for the notification
        person - name of the person with whom we are corresponding
        org - name of the organization for the account
        org_id - primary key for the account's organization
        type - code for the type of account (e.g., A (Active))
        type_string - see Partner.TYPE_STRINGS
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
    TYPE_STRINGS = { "T": "Test", "A": "Active", "S": "Special" }
    HOST = cdr.APPC
    UPDATE_URL = "https://%s/cgi-bin/cdr/update-pdq-contact.py" % HOST
    DELAY = 3

    def __init__(self, control, node):
        """
        Extract the partner information from the XML node.

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
        self.display = u"%s at %s <%s>" % (self.person, self.org, self.email)
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

        if self.notified >= str(self.control.job.started):
            self.logger.info(u"%s notified %s", self.display, self.notified)
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

        XXX Ask Volker about instructions which describe logging
        in to sFTP server with a password (instead of using SSH keys).
        """

        summary = u"%s: %s" % (self.type_string, self.display)
        if self.expiring:
            summary += u" (warning notice sent)"
        self.logger.info("notifying %s", self.display)
        self.report(summary)
        self.send(self.job.subject, self.job.message)
        self.update("notified", self.contact_id)

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
        subject = u"Warning notice: NCI PDQ Test Account for %s" % self.org
        subject = u"%s, %s" % (subject, self.job.date_and_week())
        template = cdr.getControlValue("Publishing", "test-partner-warning")
        message = template.replace("@@EXPIRING@@", str(self.expiring)[:10])
        self.send(subject, message)
        self.notify_ops(subject, message)

    def disable(self):
        """
        Disable the account, and send a notice of expiration.

        Also, add a line to the summary report to be sent to the operators,
        and send a separate message immediately to the operators, containing
        the warning message we just sent to the data partner.

        N.B.: If processing fails between the step to send the expiration
        notice and the actual expiration itself, the partner will get a
        second notice when we correct the cause of the failure and resume
        processing. Very unlikely to happen.
        """

        self.logger.info("disabling %s", self.display)
        self.report("Disabled test account for %s" % self.display)
        subject = u"Expiration notice: NCI PDQ Test Account for %s" % self.org
        subject = u"%s, %s" % (subject, self.job.date_and_week())
        message = cdr.getControlValue("Publishing", "test-partner-disabled")
        self.send(subject, message)
        self.update("expired", self.org_id)
        self.notify_ops(subject, message)

    def report(self, line):
        """
        Append a line to the summary report file for the operators.
        """

        if isinstance(line, unicode):
            line = line.encode("utf-8")
        with open(self.control.job.report_path, "a") as fp:
            fp.write("<li>%s</li>\n" % cgi.escape(line))

    def update(self, action, id):
        """
        Update data partner's DB records to reflect notification/deactivation.

        Note: logically, we should be using the PUT verb, but either IIS
        is refusing to accept PUT requests, or the CGI module is not handling
        them properly. So we use a GET request instead.

        Pass:
            action - "notified" or "expired"
            id - primary key for record to be updated
        """

        url = "%s?action=%s&id=%s" % (self.UPDATE_URL, action, id)
        result = requests.get(url).text
        self.logger.info("%s: %s", url, result.strip())

    def send(self, subject, message):
        """
        Send an email message to this data partner's contact.
        """

        recip = u"%s <%s>" % (self.person, self.email)
        self.control.send(recip, subject, message)

    def notify_ops(self, subject, message):
        """
        Send an copy of a warning or expiration message to the operators.
        """

        partner = cgi.escape(self.display)
        lead = u"The following message was sent to %s" % partner
        message = u"<p><i>%s</i></p>%s" % (lead, message)
        self.control.send(self.control.ops, subject, message)
        self.logger.debug("copied ops on %r", subject)

    @classmethod
    def get_partners(cls, control):
        """
        Fetch the list of partners which have not already expired.
        """

        url = "https://%s/cgi-bin/cdr/get-pdq-contacts.py" % cls.HOST
        url = "%s?p=%s" % (url, "TEST" if control.test else "CDR")
        control.logger.info("fetching partners from %r", url)
        root = etree.fromstring(requests.get(url).content)
        partners = [cls(control, node) for node in root.findall("contact")]
        control.logger.info("%d partners fetched", len(partners))
        return partners

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    choices = "test", "live"
    parser.add_argument("--mode", choices=choices, required=True)
    parser.add_argument("--level", default="debug")
    parser.add_argument("--job", type=int)
    parser.add_argument("--superlog", action="store_true")
    opts = vars(parser.parse_args())
    opts["log-level"] = opts["level"]
    Notify(opts, {}).Perform()
