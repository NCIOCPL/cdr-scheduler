"""
Find the latest trial documents in the ClinicalTrials.gov database
which are relevant to cancer and record them in the CDR database.
Optional command-line argument can be given to specify how far back
to go for trials (using ISO format YYYY-MM-DD). Defaults to 21 days
before the latest first_received value in the ctgov_trial table,
or 2015-01-01, if the table is empty (that is, we're running for
the first time).

JIRA::OCECDR-3877
JIRA::OCECDR-4096
"""

import cdr
from cdrapi import db
import datetime
import lxml.etree as etree
import requests
import zipfile
from .cdr_task_base import CDRTask
from core.exceptions import TaskException
from .task_property_bag import TaskPropertyBag


class RefreshTask(CDRTask):
    """
    Implements subclass for managing the nightly refresh of trial info.
    """

    def __init__(self, parms, data):
        """
        Initialize the base class then instantiate our Control object,
        which does all the real work. The data argument is ignored.
        """

        CDRTask.__init__(self, parms, data)
        self.control = Control(parms)

    def Perform(self):
        "Hand off the real work to the Control object."
        self.control.run()
        return TaskPropertyBag()


class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.

    Class constants:

    DESCRIPTION   String describing what the script does (for help)
    NAME          Used for naming logs and zipfiles
    ZIPFILE       We save the payload from NLM here.
    """

    DESCRIPTION = "Get recent CT.gov Protocols"
    NAME = "RecentCTGovProtocols"
    ZIPFILE = cdr.BASEDIR + "/Output/%s.zip" % NAME

    def __init__(self, options):
        """
        Find out how far back to ask for trials, and create logging and
        database query objects.
        """

        cutoff = options.get("cutoff")
        if cutoff:
            self.cutoff = self.parse_date(cutoff)
        else:
            self.cutoff = self.get_default_cutoff()
        self.logger = cdr.Logging.get_logger(self.NAME)
        self.conn = db.connect(as_dict=True)
        self.cursor = self.conn.cursor()

    def run(self):
        """
        Processing involves two steps:

            1. Get the new trials from NLM.
            2. Record the ones we don't know about yet.
        """
        try:
            if self.fetch():
                self.record()
        except Exception as e:
            self.logger.exception("failed")
            raise TaskException("failed: %s" % e)

    def fetch(self):
        """
        Fetch the cancer trials added since a certain point in time.
        Save them to disk instead of just processing them in memory,
        so we can examine what we got if something unexpected goes
        wrong.

        Returns True if we got any trials; otherwise False.
        """

        # Assemble the query parameters.
        self.logger.info("fetching trials added on or after %s", self.cutoff)
        conditions = ['cancer', 'lymphedema', 'myelodysplastic syndromes',
                      'neutropenia', 'aspergillosis', 'mucositis']
        diseases = ['cancer', 'neoplasm']
        sponsor = "(National Cancer Institute) [SPONSOR-COLLABORATORS]"
        conditions = "(%s) [CONDITION]" % " OR ".join(conditions)
        diseases = "(%s) [DISEASE]" % " OR ".join(diseases)
        term = "term=%s OR %s OR %s" % (conditions, diseases, sponsor)
        cutoff = self.cutoff.strftime("&rcv_s=%m/%d/%Y")
        params = "%s%s&studyxml=true" % (term, cutoff)
        params = params.replace(" ", "+")

        # Submit the request to NLM's server.
        base  = "http://clinicaltrials.gov/ct2/results"
        url = "%s?%s" % (base, params)
        self.logger.info(url)
        try:
            response = requests.get(url)
            bytes = response.content
            if not bytes:
                self.logger.warn("empty response (no trials?)")
                return False
        except Exception as e:
            error = "Failure downloading trial set using %s: %s" % (url, e)
            raise Exception(error)

        # Save the response's payload for further processing.
        fp = open(self.ZIPFILE, "wb")
        fp.write(bytes)
        fp.close()

        # Yes, we got trials.
        return True

    def record(self):
        """
        Parse the trial documents and record the ones we don't already have.
        """

        rows = db.Query("ctgov_trial", "nct_id").execute(self.cursor)
        nct_ids = set([row["nct_id"].upper() for row in rows])
        zf = zipfile.ZipFile(self.ZIPFILE)
        names = zf.namelist()
        loaded = 0
        for name in names:
            try:
                xml = zf.read(name)
                trial = Trial(xml)
                if trial.nct_id and trial.nct_id.upper() not in nct_ids:
                    self.logger.info("adding %s", trial.nct_id)
                    nct_ids.add(trial.nct_id.upper())
                    self.cursor.execute("""\
INSERT INTO ctgov_trial (nct_id, trial_title, trial_phase, first_received)
     VALUES (?, ?, ?, ?)""", (trial.nct_id, trial.title[:1024],
                              trial.phase and trial.phase[:20] or None,
                              trial.first_received))
                    position = 1
                    for other_id in trial.other_ids:
                        self.cursor.execute("""\
INSERT INTO ctgov_trial_other_id (nct_id, position, other_id)
     VALUES (?, ?, ?)""", (trial.nct_id, position, other_id[:1024]))
                        position += 1
                    position = 1
                    for sponsor in trial.sponsors:
                        self.cursor.execute("""\
INSERT INTO ctgov_trial_sponsor (nct_id, position, sponsor)
     VALUES (?, ?, ?)""", (trial.nct_id, position, sponsor[:1024]))
                        position += 1
                    self.conn.commit()
                    loaded += 1
            except Exception as e:
                self.logger.error("%s: %s", name, e)
        self.logger.info("processed %d trials, %d new", len(names), loaded)

    @staticmethod
    def get_default_cutoff():
        """
        Default cutoff is eight days earlier than the latest date we
        have recorded for when a trial first landed on NLM's
        doortstep. If this is the very first time we've run the
        job on this server, we go all the way back to the beginning
        of 2015.
        """

        query = db.Query("ctgov_trial", "MAX(first_received) as mfr")
        rows = query.execute(as_dict=True).fetchall()
        for row in rows:
            return (row["mfr"] - datetime.timedelta(21)).date()
        return datetime.date(2015, 1, 1)

    @staticmethod
    def parse_date(date):
        """
        Convert an ISO string to a datetime.date object.
        """

        return datetime.datetime.strptime(date, "%Y-%m-%d").date()

class Trial:
    """
    Object holding information about a single clinical_trial document.

    nci_id           NLM's unique ID for the trial document
    other_ids        sequence of other (org study or secondary) IDs
    phase            string representing the current phase of the trial
    first_received   the date NLM first received the trial information
    sponsors         sequence of the names of the sponsors for the trial
    """

    def __init__(self, xml):
        """
        Parse the trial document and extract the values we need.
        """

        root = etree.XML(xml)
        self.nct_id = self.first_received = None
        self.sponsors = []
        self.other_ids = []
        for node in root.findall("id_info/*"):
            value = self.get_text(node)
            if value:
                if node.tag == "nct_id":
                    self.nct_id = value
                elif node.tag in ("org_study_id", "secondary_id"):
                    self.other_ids.append(value)
        self.title = self.get_text(root.find("brief_title"))
        if not self.title:
            self.title = self.get_text(root.findall("official_title"))
        value = self.get_text(root.find("study_first_submitted"))
        if value:
            dt = datetime.datetime.strptime(value, "%B %d, %Y")
            self.first_received = dt.date()
        self.phase = self.get_text(root.find("phase"))
        for node in root.findall("sponsors/*/agency"):
            value = self.get_text(node)
            if value:
                self.sponsors.append(value)

    @staticmethod
    def get_text(node):
        """
        Get stripped text content from a node. Assumes no mixed content.
        """
        if node is not None and node.text is not None:
            return node.text.strip()
        return None


def main():
    """
    Run the job from the command-line, allowing the user to override
    the default cutoff date.
    """

    import argparse
    fc = argparse.ArgumentDefaultsHelpFormatter
    help = "how far back to go for new trials"
    default = str(Control.get_default_cutoff())
    parser = argparse.ArgumentParser(description=Control.DESCRIPTION,
                                     formatter_class=fc)
    parser.add_argument("--cutoff", default=default, help=help)
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    Control(opts).run()

if __name__ == "__main__":
    """
    Run the job if loaded as a script (not a module).
    """

    main()
