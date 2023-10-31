"""
Find the latest trial documents in the ClinicalTrials.gov database
which are relevant to cancer and record them in the CDR database.
Optional command-line argument can be given to specify how far back
to go for trials (using ISO format YYYY-MM-DD). Defaults to 365 days
before the latest first_received value in the ctgov_trial table,
or 2015-01-01, if the table is empty (that is, we're running for
the first time).

JIRA::OCECDR-3877
JIRA::OCECDR-4096
JIRA::OCECDR-5264 - Switch to NLM's new API (old one is broken)
"""

from cdr import Logging
from cdrapi import db
from datetime import date, datetime, timedelta
from functools import cached_property
from requests import get
from .base_job import Job


class RefreshTask(Job):
    """
    Implements subclass for managing the nightly refresh of trial info.
    """

    SUPPORTED_PARAMETERS = {"cutoff"}

    def run(self):
        "Hand off the real work to the Control object."
        Control(self.opts).run()


class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can provide a way to run this task from the command line.

    Class constants:

    BASE          Base URL for NLM's clinical trials service
    CONDITIONS    Strings for conditions we want to search
    DISEASES      Strings for diseases we want to search
    SPONSOR       String for NCI as sponsor
    DESCRIPTION   String describing what the script does (for help)
    FIELDS        Fields we want NLM to return
    NAME          Used for naming log
    """

    BASE = "https://clinicaltrials.gov/api/v2/studies"
    CONDITIONS = (
        "cancer",
        "lymphedema",
        "myelodysplastic+syndromes",
        "neutropenia",
        "aspergillosis",
        "mucositis",
        "neoplasm",
    )
    DISEASES = "cancer", "neoplasm"
    FIELDS = (
        "NCTId",
        "OrgStudyId",
        "SecondaryId",
        "OfficialTitle",
        "BriefTitle",
        "Phase",
        "StudyFirstSubmitDate",
        "SponsorCollaboratorsModule",
    )
    SPONSOR = "National Cancer Institute"
    DESCRIPTION = "Get recent CT.gov Protocols"
    NAME = "RecentCTGovProtocols"

    def __init__(self, options):
        """Remember the caller's options.

        options - dictionary of run-time options
        """

        self.__options = options
        self.oldest = None

    def run(self):
        """
        Processing involves two steps:

            1. Get the new trials from NLM.
            2. Record the ones we don't know about yet.
        """

        try:
            if self.new_trials:
                self.record()
                if self.oldest:
                    message = "earliest new trial date is %s"
                    self.logger.info(message, self.oldest)
        except Exception:
            self.logger.exception("failed")
            raise

    @cached_property
    def conn(self):
        """Connection to the CDR database."""
        return db.connect()

    @cached_property
    def cursor(self):
        """For running database queries."""
        return self.conn.cursor()

    @cached_property
    def cutoff(self):
        """How far back should we go to fetch trials?"""

        cutoff = self.__options.get("cutoff")
        if cutoff:
            return datetime.strptime(cutoff, "%Y-%m-%d").date()
        query = db.Query("ctgov_trial", "MAX(first_received) AS mfr")
        rows = query.execute(self.cursor).fetchall()
        for row in rows:
            return (row.mfr - timedelta(365)).date()
        return date(2015, 1, 1)

    @cached_property
    def logger(self):
        """For recording what we do."""
        return Logging.get_logger(self.NAME)

    @cached_property
    def new_trials(self):
        """Clinical trials we don't already have."""

        # Assemble the query parameters.
        self.logger.info("fetching trials added on or after %s", self.cutoff)
        conditions = f"AREA[Condition]({' OR '.join(self.CONDITIONS)})"
        diseases = f"AREA[ConditionSearch]({' OR '.join(self.DISEASES)})"
        sponsor = f"AREA[SponsorSearch]({self.SPONSOR})"
        cutoff = f"AREA[StudyFirstSubmitDate]RANGE[{self.cutoff},MAX]"
        term = f"({conditions} OR {diseases} OR {sponsor}) AND {cutoff}"
        fields = ",".join(self.FIELDS)
        params = f"query.term={term}&fields={fields}&pageSize=1000"
        url = f"{self.BASE}?{params}"
        self.logger.info(url)

        # Loop to fetch all of the trials.
        trials = []
        fetched = 0
        done = False
        token = None
        while not done:
            try:
                response = get(f"{url}&pageToken={token}" if token else url)
                values = response.json()
                studies = values["studies"]
                token = values.get("nextPageToken")
            except Exceptions:
                args = response.reason, response.text
                self.logger.exception("reason=%s response=%s", *args)
                raise Exception(f"Unable to fetch trials for {url}")
            for study in studies:
                fetched += 1
                trial = Trial(study)
                if trial.nct_id and trial.nct_id not in self.old_trials:
                    trials.append(trial)
                    self.old_trials.add(trial.nct_id)
                    if trial.first_received:
                        if self.oldest is None:
                            self.oldest = trial.first_received
                        elif trial.first_received < self.oldest:
                            self.oldest = trial.first_received
            if not token:
                done = True
        self.logger.info("processed %d trials, %d new", fetched, len(trials))
        return trials

    @cached_property
    def old_trials(self):
        """NCT IDs for clinical trials we already have."""

        rows = db.Query("ctgov_trial", "nct_id").execute(self.cursor)
        return {row.nct_id.upper() for row in rows}

    def record(self):
        """Save new trials to the database."""

        loaded = 0
        for trial in self.new_trials:
            try:
                self.logger.info("adding %s", trial.nct_id)
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
            except Exception:
                self.logger.exception(trial.nct_id)
        self.logger.info("loaded %d new trials", loaded)


class Trial:
    """
    Object holding information about a single clinical_trial document.

    nct_id           NLM's unique ID for the trial document
    other_ids        sequence of other (org study or secondary) IDs
    phase            string representing the current phase of the trial
    first_received   the date NLM first received the trial information
    sponsors         sequence of the names of the sponsors for the trial
    """

    PHASES = dict(
        NA="N/A",
        EARLY_PHASE1="Early Phase 1",
        PHASE1="Phase 1",
        PHASE2="Phase 2",
        PHASE3="Phase 3",
        PHASE4="Phase 4",
    )

    def __init__(self, values):
        """Remember the values we got from NLM for the trial.

        values - dictionary of values received from NLM
        """

        self.__values = values or {}

    @cached_property
    def first_received(self):
        """When the trial was first submitted to NLM."""

        module = self.protocol.get("statusModule")
        return module.get("studyFirstSubmitDate") if module else None

    @cached_property
    def identification(self):
        """Information about the trial's IDs."""
        return self.protocol.get("identificationModule", {})

    @cached_property
    def id_info(self):
        """Organization study ID information."""
        return self.identification.get("orgStudyIdInfo", {})

    @cached_property
    def nct_id(self):
        """NLM's unique ID for the trial."""
        return self.identification.get("nctId")

    @cached_property
    def other_ids(self):
        """Alternate IDs for the trial."""

        other_ids = []
        org_study_id = self.id_info.get("id", "").strip()
        if org_study_id:
            other_ids = [org_study_id]
        for id_info in self.identification.get("secondaryIdInfos", []):
            secondary_id = id_info.get("id", "").strip()
            if secondary_id:
                other_ids.append(secondary_id)
        return other_ids

    @cached_property
    def phase(self):
        """Mapped string value for the phase(s) we find."""

        phases = []
        module = self.protocol.get("designModule")
        if module and "phases" in module:
            for phase in module["phases"]:
                phase = phase.strip()
                if phase:
                    phases.append(self.PHASES.get(phase, phase))
        return "/".join(sorted(phases)) or None

    @cached_property
    def protocol(self):
        """Wrapper for the trial's information."""
        return self.__values.get("protocolSection", {})

    @cached_property
    def sponsors(self):
        """Sequence of strings for the trial's sponsors."""

        sponsors = []
        module = self.protocol.get("sponsorCollaboratorsModule")
        if module:
            if "leadSponsor" in module:
                name = module["leadSponsor"].get("name", "").strip()
                if name:
                    sponsors = [name]
            if "collaborators" in module:
                for collaborator in module["collaborators"]:
                    name = collaborator.get("name", "").strip()
                    if name:
                        sponsors.append(name)
        return sponsors

    @cached_property
    def title(self):
        """Briefest title we can find for the trial."""

        title = self.identification.get("briefTitle", "").strip()
        if not title:
            title = self.identification.get("officialTitle", "").strip()
        return title or None


def main():
    """
    Run the job from the command-line, allowing the user to override
    the default cutoff date.
    """

    import argparse
    fc = argparse.ArgumentDefaultsHelpFormatter
    help = "how far back to go for new trials"
    parser = argparse.ArgumentParser(description=Control.DESCRIPTION,
                                     formatter_class=fc)
    parser.add_argument("--cutoff", help=help)
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    Control(opts).run()


if __name__ == "__main__":
    """Run the job if loaded as a script (not a module)."""
    main()
