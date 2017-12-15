"""
Perform scheduled updates of electronic mailer tracking documents.
"""

import lxml.etree as etree
import requests
import cdr
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class UpdateTask(CDRTask):
    """
    Implements subclass for managing scheduled CDR mailer tracking update
    job.
    """

    LOGNAME = "mailer"

    def Perform(self):
        "Hand off the real work to the Mailer class."
        Mailer.run(self.logger)
        return TaskPropertyBag()


class Mailer:
    """
    Object for a single mailer's document.
    """

    EMAILERSWEB = "https://%s/cgi-bin" % cdr.Tier().hosts["EMAILERSWEB"]
    "Base URL for this tier's emailer web server."

    XML_TO_STRING_OPTS = {
        "pretty_print": True,
        "encoding": "utf-8",
        "xml_declaration": True
    }
    "Options for serializing the XML mailer tracking document."

    COMMENT = "Automatic update of electronic mailer tracking document"
    "Passed to cdr.repDoc() as comment and reason arguments."

    REP_OPTS = {
        "comment": COMMENT,
        "reason": COMMENT,
        "ver": "Y",
        "checkIn": "Y",
        "val": "Y",
        "showWarnings": True
    }
    "Options used in call to cdr.repDoc()."

    def __init__(self, node, session, logger):
        """
        Find out what needs to be recorded for the mailer and retrieve
        the current working document for its CDR tracking document (if
        there's anything to record).
        """

        self.id = node.get("id")
        self.session = session
        self.logger = logger
        self.changes = "None"
        completed = node.get("completed")
        bounced = node.get("bounced")
        expired = node.get("expired")
        modified = node.get("modified") == "Y"
        self.date = completed or bounced or expired or None
        if not self.date:
            raise Exception("no disposition recorded for mailer")
        if completed:
            if modified:
                self.changes = "Administrative changes"
        elif bounced:
            self.changes = "Returned to sender"
        response = cdr.getDoc(session, self.id, "Y", getObject=True)
        if isinstance(response, basestring):
            raise Exception(u"getDoc(): %s" % response)
        self.doc = response
        self.root = etree.XML(self.doc.xml)

    def update_tracker(self):
        """
        Add the disposition to the tracker document.
        Save the modified document.
        Tell the emailer server we've recorded the disposition.
        """

        self.doc.xml = self.transform()
        response = cdr.repDoc(self.session, doc=str(self.doc), **self.REP_OPTS)
        if not response[0]:
            message = response[1] or u"unexpected failure"
            raise Exception(u"repDoc(): %s" % message)
        if response[1]:
            self.logger.warn("tracker %s: %s", self.id, response[1])
        self.logger.info("updated tracking document %s", self.id)
        self.record_update()

    def unlock(self, reason):
        "Check the document back in, releasing the editing lock."
        cdr.unlock(self.session, cdr.normalize(self.id), reason=reason)

    def already_updated(self):
        "Find out if we've already done the work for this mailer."
        for node in self.root.findall("Response"):
            self.logger.warn("tracker document %s already updated", self.id)
            return True
        return False

    def transform(self):
        "Append the <Response> element to the document."
        response = etree.Element("Response")
        etree.SubElement(response, "Received").text = self.date[:10]
        etree.SubElement(response, "ChangesCategory").text = self.changes
        self.root.append(response)
        return etree.tostring(self.root, **self.XML_TO_STRING_OPTS)

    def record_update(self):
        """
        Update the row in the emailer server's database table for the mailer
        to reflect that the mailer response has been recorded in the CDR's
        tracking document.
        """

        data = { "mailerId": self.id, "recorded": self.date }
        url = "%s/recorded-gp.py" % self.EMAILERSWEB
        response = requests.post(url, data, verify=False)
        if not response.text.startswith("OK"):
            self.logger.warn("mailer %s: %s", mailerId, response.text)

    @classmethod
    def run(cls, logger):
        """
        Ask the emailer server for a list of the mailers whose
        disposition needs to be recorded, and update the tracker
        document for each one.
        """

        logger.info("emailer_tracking job started")

        # Log into the CDR server with a local machine account.
        session = cdr.login("etracker", cdr.getpw("etracker"))

        # Get the list of mailers from the emailer server.
        url = "%s/completed-gp.py" % cls.EMAILERSWEB
        response = requests.get(url, verify=False)
        tree = etree.XML(response.text)

        # Walk through the list and update each one.
        for node in tree.findall("mailer"):
            try:
                mailer = Mailer(node, session, logger)
                if mailer.already_updated():
                    mailer.unlock("Tracker already updated")
                else:
                    mailer.update_tracker()
            except Exception, e:
                logger.error("failure for mailer %s: %s", node.get("id"), e)

        # Clean up and go home.
        cdr.logout(session)
        logger.info("emailer_tracking job completed")


if __name__ == "__main__":
    "Make it possible to run this task from the command line."
    import logging
    logging.basicConfig(format=cdr.Logging.FORMAT, level=logging.INFO)
    Mailer.run(logging.getLogger())
