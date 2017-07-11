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
"""

import re
import socket
from lxml import etree
import cdr
import cdrdb2 as cdrdb
from cdr_task_base import CDRTask
from task_property_bag import TaskPropertyBag

class Task(CDRTask):
    """Implements subclass to repopulate the glossifier mappings.
    """

    LOGNAME = "glossifier"

    def Perform(self):
        log_level = self.jobParams.get("log-level", "info")
        self.logger = cdr.Logging.get_logger(self.LOGNAME, level=log_level)
        terms = Terms(self.logger)
        terms.save()
        if terms.dups:
            terms.report_duplicates()
        return TaskPropertyBag()

class Terms:

    SERVER = socket.gethostname().split(".")[0]
    SENDER = "cdr@%s.nci.nih.gov" % SERVER.lower()
    SUBJECT = "DUPLICATE GLOSSARY TERM NAME MAPPINGS ON %s" % SERVER.upper()

    def __init__(self, logger=None):
        """Collect the glossary term information.
        """

        self.logger = logger
        if self.logger is None:
            self.logger = cdr.Logging.get_logger("glossifier", level="debug")
        self.conn = cdrdb.connect()
        self.cursor = self.conn.cursor()
        self.usages = self.collect_usages()

    def save(self):
        """Store the serialized name information in the database.
        """

        names = repr(self.collect_names())
        self.logger.info("saving glossifier names (%d bytes)", len(names))
        self.cursor.execute("""\
            UPDATE glossifier
               SET refreshed = GETDATE(),
                   terms = %s
             WHERE pk = 1""", names)
        self.conn.commit()

    def collect_usages(self):
        """Fetch published glossary term name documents.

        Return value is a dictionary indexed by a tuple containing:
          - normalized term name string
          - language ("en" or "es")
          - dictionary (e.g., "Cancer.gov"; None if no dictionaries
                        assigned for this language)
        The values of the dictionaries are sequence of glossary term
        name documents which are found for the tuple's values. In order
        to be usable by the glossifier, each value must be unique
        (that is, the sequence must have exactly one term name doc ID).
        """

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
            extra = { "en": None, "es": None }

            def __init__(self, terms, term_id, doc_xml, concept):
                """Parse the document to get its names.

                Only use name strings which have not be rejected, or
                marked as excluded from the glossifier. the list of
                names is augmented from the external_map table.
                """

                self.terms = terms
                self.id = term_id
                self.concept = concept
                self.names = { "en": [], "es": [] }
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
                    self.names[language] += self.get_extra_names(language)

            def get_extra_names(self, language):
                """Fetch variant names from the external_map table.

                Optimized to do this once per language, as it increases
                the performance time by a considerable amount when we use
                two separate queries for each term name document.
                """

                if self.extra[language] is None:
                    usage = self.USAGES[language]
                    query = cdrdb.Query("external_map m", "m.value", "m.doc_id")
                    query.join("external_map_usage u", "u.id = m.usage")
                    query.where(query.Condition("u.name", usage))
                    rows = query.execute(self.terms.cursor).fetchall()
                    self.terms.logger.debug("fetched %d extra %s names",
                                            len(rows), language)
                    extra = {}
                    for name, doc_id in rows:
                        if doc_id not in extra:
                            extra[doc_id] = [name]
                        else:
                            extra[doc_id].append(name)
                    self.extra[language] = extra
                    #open("extra.%s" % language, "w").write(repr(extra))
                names = self.extra[language].get(self.id, [])
                return [self.Name(name) for name in names]

            def record_usages(self, usages):
                """Record language/dictionary combos this term is used for.
                """

                for lang in self.names:
                    for name in self.names[lang]:
                        if self.concept and self.concept.dictionaries[lang]:
                            for dct in self.concept.dictionaries[lang]:
                                self.record_usage(usages, name, lang, dct)
                        else:
                            self.record_usage(usages, name, lang)

            def record_usage(self, usages, name, language, dictionary=None):
                """Add CDR term name ID to sequence for this name usage.
                """

                key = (name.key, language, dictionary)
                if key not in usages:
                    usages[key] = set([self.id])
                else:
                    usages[key].add(self.id)

            class Name:
                """Holds original and normalized version of name string.
                """

                def __init__(self, value):
                    self.value = value
                    value = value.replace('\u2019', u"'").lower().strip()
                    self.key = re.sub(u"\\s+", " ", value)

        # Start with an empty usages dictionary.
        usages = {}

        # Get the dictionary of Concept object with dictionary information.
        concepts = self.fetch_concepts()
        self.logger.debug("fetched %d concepts", len(concepts))

        # Fetch all of the published CDR glossary term documents.
        query = cdrdb.Query("doc_version v", "v.id", "v.xml", "q.int_val")
        query.join("pub_proc_doc d", "d.doc_id = v.id", "d.doc_version = v.num")
        query.join("pub_proc_cg c", "c.id = v.id", "c.pub_proc = d.pub_proc")
        query.join("query_term_pub q", "q.doc_id = v.id")
        query.where("q.path = '/GlossaryTermName/GlossaryTermConcept/@cdr:ref'")
        rows = query.execute(self.cursor).fetchall()
        self.logger.debug("processing %d glossary terms", len(rows))

        # Use the term information to populate the usages dictionary.
        for term_id, doc_xml, concept_id in rows:
            term = Term(self, term_id, doc_xml, concepts.get(concept_id))
            term.record_usages(usages)
        return usages

    def collect_names(self):
        """Build the dictionary of name information used by the glossifier.

        Only unique usage information is included in the returned dictionary.
        Duplicate usage is stored in the `dups` attribute as a side effect
        of this method, so that they can be reported via email notification.
        There are a handful of unreported duplicates which CIAT has decided
        not to eliminate.

        Return:
            nested dictionary indexed by normalized name strings:
                names[normalized-name][doc_id][language] => set of dictionaries
        """

        unreported = set(["tpa", "cab", "ctx", "receptor"])
        self.dups = {}
        names = {}
        for key in self.usages:
            name, language, dictionary = key
            ids = list(self.usages[key])
            if len(ids) > 1:
                if name not in unreported:
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
        return names

    def fetch_concepts(self):
        """Collect dictionary information for the term concepts.
        """

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
                self.dictionaries = { "en": set(), "es": set() }

        concepts = {}
        tags = { "en": "TermDefinition", "es": "TranslatedTermDefinition" }
        for lang in tags:
            path = "/GlossaryTermConcept/%s/Dictionary" % tags[lang]
            query = cdrdb.Query("query_term_pub", "doc_id", "value")
            query.where(query.Condition("path", path))
            rows = query.execute(self.cursor).fetchall()
            self.logger.debug("fetched %d %s dictionaries", len(rows), lang)
            for doc_id, dictionary in rows:
                concept = concepts.get(doc_id)
                if not concept:
                    concept = concepts[doc_id] = Concept(doc_id)
                concept.dictionaries[lang].add(dictionary.strip())
        return concepts

    def report_duplicates(self):
        """Send a report on duplicate name+language+dictionary mappings.
        """

        if not self.dups:
            self.logger.error("no duplicates to report")
            return
        recips = CDRTask.get_group_email_addresses("GlossaryDupGroup")
        if not recips:
            raise Exception("no recipients found for glossary dup message")
        body = [u"The following %d sets of duplicate " % len(self.dups),
                u"glossary mappings were found in the CDR ",
                "on %s. " % self.SERVER.upper(),
                u"Mappings for any phrase + language + dictionary must ",
                u"be unique. ",
                u"Please correct the data so that this requirement is met. ",
                u"You may need to look at the External Map Table for ",
                u"Glossary Terms to find some of the mappings.\n"]
        for key in sorted(self.dups):
            name, language, dictionary = key
            body.append(u"\n%s (language=%r dictionary=%r)\n" %
                        (name.upper(), language, dictionary))
            for doc_id in self.dups[key]:
                body.append(u"\tCDR%010d\n" % doc_id)
        body = u"".join(body)
        cdr.sendMail(self.SENDER, recips, self.SUBJECT, body, False, True)
        self.logger.info("duplicate mapping notification sent to %r", recips)

if __name__ == "__main__":
    """Support command-line testing.
    """

    import argparse
    import pprint
    parser = argparse.ArgumentParser()
    parser.add_argument("--show-dups", action="store_true")
    opts = parser.parse_args()
    terms = Terms()
    t = pprint.pformat(terms.collect_names(), indent=4)
    print re.sub(r"set\(\[\s+", "set([", t).replace(" u'es'", " 'es'")
    if opts.show_dups:
        print "=" * 60
        print "DUPLCATES"
        print "=" * 60
        for key in terms.dups:
            print repr((key, terms.dups[key]))
