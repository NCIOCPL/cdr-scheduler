"""Refresh the terms dictionary used by the glossifier service.

This is a rewritten version of the script which ran on Linux.
There will be some differences in the glossification results.
Some name strings from the external map table were incorrectly
skipped in the Linux version when the name(s) from the CDR document
were not included (because of the ExcludeFromGlossifier attribute
on those names). Also some of the name strings were pulled from
the query_term_pub table by the Linux script, and that table only
stored 8-bit character strings, so some of the Unicode strings
were mangled. The current version pulls the names from the original
GlossaryTermName document (and the external map, which stores true
Unicode values).

The rewrite is part of an effort to reduce the footprint of the CDR
system. Moving the glossifier service to the CDR Windows server is
a step toward eliminating the Linux server on which it used to run.

The wheel of life turns again. Now, in 2019, we are moving the
glossifier service to the individual Drupal CMS sites. This job
now pushes nightly refreshes of the PDQ glossary to the servers
for each of those sites.
"""

import argparse
import json
import pprint
import re
import socket
from lxml import etree
import requests
import cdr
from cdrapi import db
from cdrapi.settings import Tier
from .base_job import Job


class Task(Job):
    """
    Implements subclass to repopulate the glossifier mappings.
    """

    LOGNAME = "glossifier"
    SUPPORTED_PARAMETERS = {"log-level", "recip"}

    def run(self):
        level = self.opts.get("log-level", "INFO")
        self.logger.setLevel(level.upper())
        terms = Terms(self.logger, self.opts.get("recip"))
        terms.send()
        terms.save()
        if terms.dups:
            terms.report_duplicates()


class Terms:

    SERVER = socket.gethostname().split(".")[0]
    SENDER = "cdr@{}.nci.nih.gov".format(SERVER.lower())
    SUBJECT = "DUPLICATE GLOSSARY TERM NAME MAPPINGS ON " + SERVER.upper()
    UNREPORTED = set()  # OCECDR-4795 set(["tpa", "cab", "ctx", "receptor"])
    GROUP = "glossary-servers"

    def __init__(self, logger=None, recip=None):
        """
        Collect the glossary term information.

        Pass:
            logger - the scheduled job's logger (unless testing from the
                     command line)
            recip - optional email address for testing without spamming
                    the users
        """

        self.tier = Tier()
        self.logger = logger
        self.recip = recip
        if self.logger is None:
            self.logger = cdr.Logging.get_logger("glossifier", level="debug")
        self.conn = db.connect()
        self.cursor = self.conn.cursor()

    def save(self):
        """
        Store the serialized name information in the database.
        """

        names = repr(self.names)
        self.logger.info("saving glossifier names (%d bytes)", len(names))
        self.cursor.execute("""\
            UPDATE glossifier
               SET refreshed = GETDATE(),
                   terms = ?
             WHERE pk = 1""", names)
        self.conn.commit()

    def send(self):
        """
        Send the glossary information to registered Drupal CMS servers
        """

        failures = []
        success = "Sent glossary to server %r at %s"
        failure = "Failure sending glossary to server %r at %s: %s"
        for alias, base in self.servers.items():
            url = "{}/pdq/api/glossifier/refresh".format(base)
            try:
                response = requests.post(url, json=self.data, auth=self.auth)
                if response.ok:
                    self.logger.info(success, alias, base)
                else:
                    args = alias, base, response.reason
                    self.logger.error(failure, *args)
                    failures.append(args)
            except Exception as e:
                args = alias, base, e
                self.logger.exception(failure, *args)
                failures.append(args)
        if failures:
            group = "Developers Notification"
            if self.recip:
                recips = [self.recip]
            else:
                recips = Job.get_group_email_addresses(group)
            if not recips:
                raise Exception("no recips found for glossary failure message")
            tier = self.tier.name
            subject = "[{}] Failure sending glossary information".format(tier)
            lines = []
            for args in failures:
                lines.append("Server {!r} at {}: {}".format(*args))
            body = "\n".join(lines)
            opts = dict(subject=subject, body=body)
            cdr.EmailMessage(self.SENDER, recips, **opts)
            self.logger.error("sent failure notice sent to %r", recips)

    @property
    def auth(self):
        """
        Basic authorization credentials pair for Drupal CMS servers
        """

        if not hasattr(self, "_auth"):
            password = self.tier.password("PDQ")
            if not password:
                raise Exception("Unable to find PDQ CMS credentials")
            self._auth = "PDQ", password
        return self._auth

    @property
    def concepts(self):
        """
        Dictionary information for the term concepts.
        """

        if not hasattr(self, "_concepts"):

            class Concept:
                """
                CDR GlossaryTermConcept document.

                Attributes:
                  - id: integer for the document's CDR ID
                  - dictionaries: English and Spanish dictionaries
                                  for which we have definitions
                """

                def __init__(self, doc_id):
                    self.id = doc_id
                    self.dictionaries = dict(en=set(), es=set())

            self._concepts = {}
            tags = dict(en="TermDefinition", es="TranslatedTermDefinition")
            for lang in tags:
                path = "/GlossaryTermConcept/{}/Dictionary".format(tags[lang])
                query = db.Query("query_term_pub", "doc_id", "value")
                query.where(query.Condition("path", path))
                rows = query.execute(self.cursor).fetchall()
                args = len(rows), lang
                self.logger.debug("fetched %d %s dictionaries", *args)
                for doc_id, dictionary in rows:
                    concept = self._concepts.get(doc_id)
                    if not concept:
                        concept = self._concepts[doc_id] = Concept(doc_id)
                    concept.dictionaries[lang].add(dictionary.strip())
        return self._concepts

    @property
    def data(self):
        """
        JSON-serializable glossary data for the Drupal CMS servers

        JSON can't deal with sets, so we transform the sets of
        dictionaries into plain lists.
        """

        if not hasattr(self, "_data"):
            names = dict()
            for name, docs in self.names.items():
                names[name] = dict()
                for doc_id, languages in docs.items():
                    names[name][doc_id] = dict()
                    for language, dictionaries in languages.items():
                        names[name][doc_id][language] = list(dictionaries)
            self._data = names
        return self._data

    @property
    def extra_names(self):
        """Fetch variant names from the external_map table."""

        if not hasattr(self, "_extra_names"):
            self._extra_names = {}
            for langcode in Term.USAGES:
                query = db.Query("external_map m", "m.value", "m.doc_id")
                query.join("external_map_usage u", "u.id = m.usage")
                query.where(query.Condition("u.name", Term.USAGES[langcode]))
                rows = query.execute(self.cursor).fetchall()
                args = len(rows), langcode
                self.logger.debug("fetched %d extra %s names", *args)
                names = {}
                for name, doc_id in rows:
                    if doc_id not in names:
                        names[doc_id] = [name]
                    else:
                        names[doc_id].append(name)
                self._extra_names[langcode] = names
        return self._extra_names

    @property
    def names(self):
        """
        Dictionary of name information used by the glossifier.

        Only unique usage information is included in the returned dictionary.
        Duplicate usage is stored in the `dups` attribute as a side effect
        of this method, so that they can be reported via email notification.
        There are a handful of unreported duplicates which CIAT has decided
        not to eliminate.

        Return:
            nested dictionary indexed by normalized name strings:
                names[normalized-name][doc_id][language] => set of dictionaries
        """

        if not hasattr(self, "_names"):
            self.dups = dict()
            names = dict()
            for key in self.usages:
                name, language, dictionary = key
                ids = list(self.usages[key])
                if len(ids) > 1:
                    if name not in self.UNREPORTED:
                        self.dups[key] = ids
                else:
                    doc_id = ids[0]
                    if name not in names:
                        names[name] = {}
                    if doc_id not in names[name]:
                        names[name][doc_id] = {}
                    if language not in names[name][doc_id]:
                        names[name][doc_id][language] = set()
                    if dictionary is not None:
                        names[name][doc_id][language].add(dictionary)
            self._names = names
        return self._names

    @property
    def servers(self):
        """
        Servers who receive scheduled updated glossary data

        This property is a dictionary of each server's base URL,
        indexed by a unique alias.

        The servers are stored in the CDR control table. Each server
        gets a row in the table, with `GROUP` as the value of the `grp`
        column, and a unique alias for the server stored in the `name`
        column. The URL for the server is stored in the `val` column.

        If no servers are found in the table, then fetch the
        DRUPAL CMS with which this tier is associated, and
        use the alias "Primary" for the server.
        """

        if not hasattr(self, "_servers"):
            self._servers = cdr.getControlGroup(self.GROUP)
            if not self._servers:
                server = self.tier.hosts.get("DRUPAL")
                self._servers = dict(Primary="https://{}".format(server))
        return self._servers

    @property
    def usages(self):
        """
        Published glossary term name documents.

        Property value is a dictionary indexed by a tuple containing:
          - normalized term name string
          - language ("en" or "es")
          - dictionary (e.g., "Cancer.gov"; None if no dictionaries
                        assigned for this language)
        The values of the dictionaries are sequence of glossary term
        name documents which are found for the tuple's values. In order
        to be usable by the glossifier, each value must be unique
        (that is, the sequence must have exactly one term name doc ID).
        """

        if not hasattr(self, "_usages"):

            # Start with an empty usages dictionary.
            self._usages = {}

            # Get the dictionary of Concept object with dictionary information.
            concepts = self.concepts
            self.logger.debug("fetched %d concepts", len(concepts))

            # Fetch all of the published CDR glossary term documents.
            columns = "v.id", "v.xml", "q.int_val"
            joins = (
                ("pub_proc_doc d", "d.doc_id = v.id", "d.doc_version = v.num"),
                ("pub_proc_cg c", "c.id = v.id", "c.pub_proc = d.pub_proc"),
                ("query_term_pub q", "q.doc_id = v.id"),
            )
            path = "/GlossaryTermName/GlossaryTermConcept/@cdr:ref"
            query = db.Query("doc_version v", *columns)
            for args in joins:
                query.join(*args)
            query.where(query.Condition("q.path", path))
            rows = query.execute(self.cursor).fetchall()
            self.logger.debug("processing %d glossary terms", len(rows))

            # Use the term information to populate the usages dictionary.
            for term_id, doc_xml, concept_id in rows:
                term = Term(self, term_id, doc_xml, concepts.get(concept_id))
                term.record_usages(self._usages)

        return self._usages

    def report_duplicates(self):
        """
        Send a report on duplicate name+language+dictionary mappings.
        """

        if not self.dups:
            self.logger.error("no duplicates to report")
            return
        if self.recip:
            recips = [self.recip]
        else:
            recips = Job.get_group_email_addresses("GlossaryDupGroup")
        if not recips:
            raise Exception("no recipients found for glossary dup message")
        body = ["The following {:d} sets of ".format(len(self.dups)),
                "duplicate glossary mappings were found in the CDR ",
                "on {}. ".format(self.SERVER.upper()),
                "Mappings for any phrase + language + dictionary must ",
                "be unique. ",
                "Please correct the data so that this requirement is met. ",
                "You may need to look at the External Map Table for ",
                "Glossary Terms to find some of the mappings.\n"]
        template = "\n{} (language={!r} dictionary={!r})\n"
        for key in sorted(self.dups):
            name, language, dictionary = key
            args = name.upper(), language, dictionary
            body.append(template.format(*args))
            for doc_id in self.dups[key]:
                body.append("\tCDR{:010d}\n".format(doc_id))
        body = "".join(body)
        opts = dict(subject=self.SUBJECT, body=body)
        message = cdr.EmailMessage(self.SENDER, recips, **opts)
        message.send()
        self.logger.info("duplicate mapping notification sent to %r", recips)


class Term:
    """
    GlossaryTermName document object.

    Attributes:
      - terms: reference to master object for term collection
      - id: integer for CDR term name document ID
      - concept: reference to concept object
      - names: two sequences of Name object, one for each language

    Note that this class's USAGES dictionary is used for finding
    other variant strings for the term name in the external map
    dictionary, and is not the same thing as the 'usages' collected
    by the Terms object.
    """

    USAGES = {
        "en": "GlossaryTerm Phrases",
        "es": "Spanish GlossaryTerm Phrases"
    }

    def __init__(self, terms, term_id, doc_xml, concept):
        """
        Parse the document to get its names.

        Only use name strings which have not be rejected, or
        marked as excluded from the glossifier. the list of
        names is augmented from the external_map table.
        """

        self.terms = terms
        self.id = term_id
        self.concept = concept
        self.names = {"en": [], "es": []}
        root = etree.fromstring(doc_xml.encode("utf-8"))
        if cdr.get_text(root.find("TermNameStatus")) != "Rejected":
            for node in root.findall("TermName"):
                if node.get("ExcludeFromGlossifier") != "Yes":
                    name = cdr.get_text(node.find("TermNameString"))
                    self.names["en"].append(self.Name(name))
        for node in root.findall("TranslatedName"):
            status = cdr.get_text(node.find("TranslatedNameStatus"))
            if status != "Rejected":
                if node.get("ExcludeFromGlossifier") != "Yes":
                    name = cdr.get_text(node.find("TermNameString"))
                    self.names["es"].append(self.Name(name))
        for language in self.USAGES:
            for name in terms.extra_names[language].get(term_id, []):
                self.names[language].append(self.Name(name))

    def record_usages(self, usages):
        """
        Record language/dictionary combos this term is used for.
        """

        for lang in self.names:
            for name in self.names[lang]:
                if self.concept and self.concept.dictionaries[lang]:
                    for dct in self.concept.dictionaries[lang]:
                        self.record_usage(usages, name, lang, dct)
                else:
                    self.record_usage(usages, name, lang)

    def record_usage(self, usages, name, language, dictionary=None):
        """
        Add CDR term name ID to sequence for this name usage.
        """

        key = (name.key, language, dictionary)
        if key not in usages:
            usages[key] = set([self.id])
        else:
            usages[key].add(self.id)

    class Name:
        """
        Holds original and normalized version of name string.
        """

        def __init__(self, value):
            self.value = value
            value = value.replace("\u2019", "'").lower().strip()
            self.key = re.sub("\\s+", " ", value)


if __name__ == "__main__":
    """
    Support command-line testing.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--show-dups", action="store_true")
    parser.add_argument("--json", action="store_true")
    opts = parser.parse_args()
    terms = Terms()
    if opts.json:
        print(json.dumps(terms.data, indent=2))
    else:
        from sys import stdout
        t = pprint.pformat(terms.names, indent=4)
        t = re.sub(r"set\(\[\s+", "set([", t).replace(" u'es'", " 'es'")
        stdout.buffer.write(t.encode("utf-8"))
        stdout.buffer.write(b"\n")
        if opts.show_dups:
            stdout.buffer.write(b"=" * 60)
            stdout.buffer.write(b"\n")
            stdout.buffer.write("DUPLICATES".center(60).encode("utf-8"))
            stdout.buffer.write(b"\n")
            stdout.buffer.write(b"=" * 60)
            stdout.buffer.write(b"\n")
            for key in terms.dups:
                dup = repr((key, terms.dups[key]))
                stdout.buffer.write(dup.encode("utf-8"))
                stdout.buffer.write(b"\n")
