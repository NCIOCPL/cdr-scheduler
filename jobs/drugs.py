"""Populate the dictionary tables with drug term information.

This is a scheduled job to replace work formerly done by GateKeeper.
This is only a temporary stop-gap so that GateKeeper can be turned
off, and will soon be replaced by the new drug dictionary API.

My proposed approach for the last bullet is to have a pair of work tables, and:
 1. TRUNCATE TABLE for the work tables
 2. populate the work tables
 3. start a transaction
 4. TRUNCATE TABLE for the live tables
 5. insert the data from the work tables into the live tables
 6. commit the transaction
"""

from argparse import ArgumentParser
from lxml import etree
from cdrapi import db
from cdrapi.docs import Doc
from cdrapi.users import Session
from .base_job import Job


class Loader(Job):

    LOGNAME = "drug-loader"
    API_VERSION = "v1"
    FILTER = "Drug Dictionary JSON"
    TABLES = "Dictionary", "DictionaryTermAlias"
    SUPPORTED_PARAMETERS = {"tier"}

    def run(self):
        """Repopulate the PDQ dictionary tables with fresh drug information."""

        for table in self.TABLES:
            self.dictionary_cursor.execute(f"TRUNCATE TABLE {table}_Work")
        self.dictionary_conn.commit()
        self.logger.info("work tables cleared")
        for id in self.ids:
            drug = self.Drug(self, id)
            if drug.wanted:
                drug.load()
        self.logger.info("work tables populated")
        for table in self.TABLES:
            insert = f"INSERT INTO {table} SELECT * FROM {table}_Work"
            self.dictionary_cursor.execute(f"TRUNCATE TABLE {table}")
            self.dictionary_cursor.execute(insert)
        self.dictionary_conn.commit()
        self.logger.info("live tables ready")

    @property
    def transform(self):
        """Compiled filter to generate JSON for the drug documents."""

        if not hasattr(self, "_transform"):
            self._transform = Doc.load_single_filter(self.session, self.FILTER)
        return self._transform

    @property
    def session(self):
        """CDR Session object for retrieving documents."""

        if not hasattr(self, "_session"):
            self._session = Session("guest")
        return self._session

    @property
    def dictionary_conn(self):
        """Connection to the PDQ dictionary tables."""

        if not hasattr(self, "_dictionary_conn"):
            opts = dict(database="pdq_dictionaries", tier=self.tier)
            self._dictionary_conn = db.connect(**opts)
        return self._dictionary_conn

    @property
    def dictionary_cursor(self):
        """DB cursor for the PDQ dictionary tables."""

        if not hasattr(self, "_dictionary_cursor"):
            self._dictionary_cursor = self.dictionary_conn.cursor()
        return self._dictionary_cursor

    @property
    def cdr_cursor(self):
        """Database cursor for the CDR tables."""

        if not hasattr(self, "_cdr_cursor"):
            opts = dict(tier=self.tier, user="CdrGuest")
            self._cdr_cursor = db.connect(**opts).cursor()
        return self._cdr_cursor

    @property
    def ids(self):
        """Sequence of ID integers for CDR drug Term documents."""

        if not hasattr(self, "_ids"):
            query = db.Query("pub_proc_cg c", "c.id").unique().order("c.id")
            query.join("query_term t", "t.doc_id = c.id")
            query.join("query_term s", "s.doc_id = t.int_val")
            query.where("t.path = '/Term/SemanticType/@cdr:ref'")
            query.where("s.path = '/Term/PreferredName'")
            query.where("s.value = 'Drug/agent'")
            rows = query.execute(self.cdr_cursor).fetchall()
            self._ids = [row.id for row in rows]
            self.logger.info("found %d drug terms", len(self._ids))
        return self._ids

    @property
    def tier(self):
        """Which database tier should we load to/from?"""

        if not hasattr(self, "_tier"):
            self._tier = self.opts.get("tier")
        return self._tier

    class Drug:
        """CDR drug Term document to be loaded."""

        DICTIONARY = "Drug"
        LANGUAGE = "English"
        SELECT_XML = "SELECT xml FROM pub_proc_cg WHERE id = ?"
        DIS = "RelatedDrugInfoSummary"
        METADATA_PATH = "/DrugInformationSummary/DrugInfoMetaData"
        URL_PATH = f"{METADATA_PATH}/URL/@cdr:xref"
        TERM_PATH = f"{METADATA_PATH}/TerminologyLink/@cdr:ref"
        DICTIONARY_INSERT = """\
INSERT INTO Dictionary_Work (
    TermID,
    TermName,
    Dictionary,
    Language,
    Audience,
    ApiVers,
    [Object]
) VALUES (?, ?, ?, ?, ?, ?, ?)"""
        ALIAS_INSERT = """\
INSERT INTO DictionaryTermAlias_Work (
    TermID,
    OtherName,
    OtherNameType,
    Language
) VALUES (?, ?, ?, ?)"""

        def __init__(self, loader, id):
            """Remember the caller's values.

            Pass:
                loader - access to the database tables and CDR login session
                id - integer for the drug Term document's CDR ID
            """

            self.loader = loader
            self.id = id

        def load(self):
            """Push the drug entry and its aliases to the database."""

            args = self.id, self.name
            self.loader.session.logger.debug("loading CDR%d (%r)", *args)
            cursor = self.loader.dictionary_cursor
            cursor.execute(self.DICTIONARY_INSERT, self.entry)
            for alias in self.aliases:
                cursor.execute(self.ALIAS_INSERT, alias)
            self.loader.dictionary_conn.commit()

        @property
        def aliases(self):
            """Sequence of other names for this drug term."""

            if not hasattr(self, "_aliases"):
                self._aliases = []
                for node in self.doc.findall("OtherName"):
                    self._aliases.append([
                        self.id,
                        Doc.get_text(node.find("OtherTermName", "")).strip(),
                        Doc.get_text(node.find("OtherNameType", "")).strip(),
                        self.LANGUAGE,
                    ])
            return self._aliases

        @property
        def audience(self):
            """String for the drug dictionary's audience."""
            return "HealthProfessional"

        @property
        def doc(self):
            """`Doc` object for the drug's Term CDR document."""

            if not hasattr(self, "_doc"):
                self.loader.cdr_cursor.execute(self.SELECT_XML, self.id)
                xml = self.loader.cdr_cursor.fetchone().xml
                self._doc = etree.fromstring(xml.encode("utf-8"))
            return self._doc

        @property
        def entry(self):
            """Sequence of values to be inserted into Dictionary_Work table."""

            if not hasattr(self, "_entry"):
                self._entry = [
                    self.id,
                    self.name,
                    self.DICTIONARY,
                    self.LANGUAGE,
                    self.audience,
                    Loader.API_VERSION,
                    self.json,
                ]
            return self._entry

        @property
        def json(self):
            """Serialized document."""

            if not hasattr(self, "_json"):
                if self.url:
                    etree.SubElement(self.doc, self.DIS).text = self.url
                result = self.loader.transform(self.doc)
                self._json = str(result)
            return self._json

        @property
        def name(self):
            """String for the drug's preferred name."""

            if not hasattr(self, "_name"):
                name = Doc.get_text(self.doc.find("PreferredName", ""))
                self._name = name.strip()
            return self._name

        @property
        def url(self):
            """URL for the linked DrugInformationSummary document, if any."""

            if not hasattr(self, "_url"):
                query = db.Query("query_term u", "u.value")
                query.join("query_term t", "t.doc_id = u.doc_id")
                query.where(f"u.path = '{self.URL_PATH}'")
                query.where(f"t.path = '{self.TERM_PATH}'")
                query.where(query.Condition("t.int_val", self.id))
                rows = query.execute(self.loader.cdr_cursor).fetchall()
                self._url = rows[0].value if rows else ""
            return self._url

        @property
        def wanted(self):
            """True only if we have a definition."""

            if not hasattr(self, "_wanted"):
                node = self.doc.find("Definition/DefinitionText")
                text = Doc.get_text(node, "").strip()
                self._wanted = True if text else False
            return self._wanted


if __name__ == "__main__":
    """Don't execute script if loaded as a module."""

    parser = ArgumentParser()
    parser.add_argument("--tier", "-t")
    opts = vars(parser.parse_args())
    Loader(None, "Drug Loader", **opts).run()
