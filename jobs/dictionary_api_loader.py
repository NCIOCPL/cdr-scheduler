"""Load dictionary nodes into ElasticSearch.
"""

from datetime import datetime
from string import ascii_lowercase
from lxml import etree, html
from lxml.html import builder
from cdr import EmailMessage
from cdrapi.docs import Doc
from dictionary_loader import DictionaryAPILoader
from .base_job import Job


class Loader(Job):
    """Processing control for the scheduler loader job."""

    LOGNAME = DictionaryAPILoader.LOGNAME
    FROM = "NCIPDQoperator@mail.nih.gov"

    def run(self):
        """Pick a dictionary class and load the terms."""

        started = datetime.now()
        if self.dictionary == "glossary":
            loader = GlossaryLoader(**self.opts)
        elif self.dictionary == "drugs":
            loader = DrugLoader(**self.opts)
        else:
            raise Exception("no dictionary specified")
        tier = loader.tier
        host = loader.host
        subject = f"[{tier}] Load of {self.dictionary} dictionary to {host}"
        try:
            loader.run()
            elapsed = datetime.now() - started
            message = f"Load completed in {elapsed}."
        except Exception as e:
            self.logger.exception("failure")
            subject += " (FAILURE)"
            message = f"Job failed: {e}\nSee logs for further details."
        self.__notify(subject, message)

    @property
    def dictionary(self):
        """String for the name of the dictionary we are loading."""
        return self.opts.get("dictionary")

    @property
    def recips(self):
        """Who we send email notifications to."""

        if not hasattr(self, "_recips"):
            recips = self.opts.get("recips")
            if recips:
                if isinstance(recips, str):
                    if "," in recips:
                        recips = [r.strip() for r in recips.split(",")]
                    else:
                        recips = [recips.strip()]
            else:
                group = "Developers Notification"
                recips = Job.get_group_email_addresses(group)
            self._recips = recips
        return self._recips

    def __notify(self, subject, message):
        """Send email notification about the job.

        Pass:
            subject - string for the email message's subject header
            message - string for the body of the message
        """

        opts = dict(subject=subject, body=message)
        message = EmailMessage(self.FROM, self.recips, **opts)
        message.send()
        self.logger.info("notification sent to %s", ", ".join(self.recips))


class GlossaryLoader(DictionaryAPILoader):

    TYPE = "glossary"
    ALIAS = "glossaryv1"
    INDEXDEF = "glossary.json"

    @property
    def ids(self):
        """Sequence of integers for the CDR documents to be loaded."""

        if not hasattr(self, "_ids"):
            query = self.Query("pub_proc_cg c", "c.id")
            query.join("document d", "d.id = c.id")
            query.join("doc_type t", "t.id = d.doc_type")
            query.where("t.name = 'GlossaryTermName'")
            rows = query.execute(self.cursor).fetchall()
            self._ids = sorted([row.id for row in rows])
        return self._ids


    class Doc(DictionaryAPILoader.Doc):
        """CDR glossary term document."""

        DEFINITIONS = "TermDefinition", "SpanishTermDefinition"
        RESOURCE_TYPES = dict(
            RelatedExternalRef="External",
            RelatedSummaryRef="Summary",
            RelatedDrugSummaryRef="DrugSummary",
            RelatedGlossaryTermRef="GlossaryTerm",
        )


        @staticmethod
        def fix(value):
            """Prepare value for inclusion in normalized key."""

            if not value:
                return "notset"
            return value.lower().replace(" ", "")

        def index(self):
            """Add information about this document to the Elasticsearch DB."""

            opts = dict(index=self.loader.index, doc_type=self.loader.doctype)
            has_spanish_definition = False
            for definition in self.definitions:
                langcode = definition.langcode
                if langcode == "es":
                    has_spanish_definition = True
                values = definition.values
                audiences = definition.audience or [None]
                dictionaries = definition.dictionary or ["NotSet"]
                for audience in audiences:
                    for dictionary in dictionaries:
                        values["audience"] = audience
                        values["dictionary"] = dictionary
                        args = (
                            self.id,
                            self.fix(dictionary),
                            langcode,
                            self.fix(audience),
                        )
                        opts["id"] = "{}_{}_{}_{}".format(*args)
                        opts["body"] = values
                        self.loader.es.index(**opts)
            if not has_spanish_definition:
                self.loader.logger.debug("%s: no spanish def", self.cdr_id)

        @property
        def definitions(self):
            """Sequence of definitions to be stored as separate ES nodes."""

            if not hasattr(self, "_definitions"):
                self._definitions = []
                for name in self.DEFINITIONS:
                    for node in self.root.findall(name):
                        self._definitions.append(Definition(self, node))
            return self._definitions

        @property
        def english_name(self):
            """English name for the glossary term."""

            if not hasattr(self, "_english_name"):
                self._english_name = Doc.get_text(self.root.find("TermName"))
            return self._english_name

        @property
        def spanish_name(self):
            """Spanish name for the glossary term."""

            if not hasattr(self, "_spanish_name"):
                tag = "SpanishTermName"
                self._spanish_name = Doc.get_text(self.root.find(tag))
            return self._spanish_name

        @property
        def related_resources(self):
            """Sequence of references to other information."""

            if not hasattr(self, "_related_resources"):
                self._related_resources = {}
                for node in self.root.findall("RelatedInformation/*"):
                    language = node.get("UseWith")
                    text = Doc.get_text(node)
                    if "Summary" in node.tag:
                        url = node.get("url")
                    elif node.tag == "RelatedExternalRef":
                        url = node.get("xref")
                    elif node.tag == "RelatedGlossaryTermRef":
                        url = None
                    if language not in self._related_resources:
                        self._related_resources[language] = []
                    self._related_resources[language].append(
                        dict(
                            text=text,
                            url=url,
                            type=self.RESOURCE_TYPES.get(node.tag),
                        )
                    )
            return self._related_resources


class Definition:
    """A language-specific definition in a CDR glossary term."""

    def __init__(self, doc, node):
        """Remember the caller's values.

        Pass:
            doc - reference to the object for the complete glossary term
            node - reference to the XML DOM object for the definition block
        """

        self.__doc = doc
        self.__node = node

    @property
    def audience(self):
        """Audience name(s) for this definition."""

        if not hasattr(self, "_audience"):
            a = [Doc.get_text(a) for a in self.__node.findall("Audience")]
            self._audience = a
        return self._audience

    @property
    def audio(self):
        """MP3 file for the term's pronunciation."""

        for node in self.__node.getparent().findall("MediaLink"):
            if node.get("type") == "audio/mpeg":
                if node.get("language") == self.langcode:
                    ref = (node.get("ref") or "").replace("CDR", "")
                    ref = ref.lstrip("0")
                    if ref:
                        return f"{ref}.mp3"
        return None

    @property
    def definition(self):
        """Plain text and html versions of the definition (same for now)."""

        text = Doc.get_text(self.__node.find("DefinitionText"))
        return dict(text=text, html=text)

    @property
    def dictionary(self):
        """Dictionary name(s) for this definition."""
        return [Doc.get_text(a) for a in self.__node.findall("Dictionary")]

    @property
    def first_letter(self):
        """Lowercase first letter of name if ascii alpha, else #."""

        if not hasattr(self, "_first_letter"):
            self._first_letter = self.term_name[0].lower()
            if self._first_letter not in ascii_lowercase:
                self._first_letter = "#"
        return self._first_letter

    @property
    def key(self):
        """Phonetic representation of the term name's pronunciation."""
        if self.langcode == "en":
            parent = self.__node.getparent()
            return Doc.get_text(parent.find("TermPronunciation"))
        return None

    @property
    def langcode(self):
        """Either 'en' or 'es' depending on the node's tag name."""

        if not hasattr(self, "_langcode"):
            self._langcode = "es" if "Spanish" in self.__node.tag else "en"
        return self._langcode

    @property
    def media(self):
        """Sequence of image or video media objects."""

        media = []
        for node in self.__node.getparent().findall("MediaLink"):
            language = node.get("language")
            if language and language != self.langcode:
                continue
            audience = node.get("audience")
            if audience:
                audience = self.__doc.AUDIENCE.get(audience, audience)
                if audience not in self.audience:
                    continue
            if node.get("type") == "image/jpeg":
                captions = []
                for child in node.findall("Caption"):
                    language = child.get("language")
                    if language == self.langcode or not language:
                        captions.append(Doc.get_text(child))
                media.append(
                    dict(
                        type="image",
                        ref=node.get("ref"),
                        alt=node.get("alt"),
                        caption=captions,
                        template=node.get("placement"),
                    )
                )
        for node in self.__node.getparent().findall("EmbeddedVideo"):
            language = node.get("language")
            if language and language != self.langcode:
                continue
            audience = node.get("audience")
            if audience:
                audience = self.__doc.AUDIENCE.get(audience, audience)
                if audience not in self.audience:
                    continue
            captions = []
            for child in node.findall("Caption"):
                language = child.get("language")
                if language == self.langcode or not language:
                    captions.append(Doc.get_text(child))
            media.append(
                dict(
                    type="video",
                    ref=node.get("ref"),
                    hosting=node.get("hosting"),
                    unique_id=node.get("unique_id"),
                    caption=captions,
                    template=node.get("template"),
                )
            )
        return media

    @property
    def pronunciation(self):
        """Audio file and optional pronunciation key for this term."""

        return dict(audio=self.audio, key=self.key)

    @property
    def term_name(self):
        """English or Spanish primary name for this glossary term."""

        if self.langcode == "en":
            return self.__doc.english_name
        return self.__doc.spanish_name

    @property
    def values(self):
        """What we send to the database for this definition."""

        if not hasattr(self, "_values"):
            related_resources = []
            for language, rr in self.__doc.related_resources.items():
                if not language or language == self.langcode:
                    related_resources += rr
            self._values = dict(
                term_id=self.__doc.id,
                term_name=self.term_name,
                first_letter=self.first_letter,
                pretty_url_name=self.__doc.clean(self.term_name),
                language=self.langcode,
                definition=self.definition,
                pronunciation=self.pronunciation,
                media=self.media,
                related_resources=related_resources,
            )
        return self._values


class DrugLoader(DictionaryAPILoader):

    TYPE = "drug"
    ALIAS = "drugv1"
    INDEXDEF = "drugs.json"

    @property
    def ids(self):
        """Sequence of integers for the CDR documents to be loaded."""

        if not hasattr(self, "_ids"):
            query = self.Query("pub_proc_cg c", "c.id").unique()
            query.join("query_term t", "t.doc_id = c.id")
            query.where("t.path = '/Term/SemanticType'")
            query.where("t.value = 'Drug/agent'")
            rows = query.execute(self.cursor).fetchall()
            self._ids = sorted([row.id for row in rows])
        return self._ids


    class Doc(DictionaryAPILoader.Doc):
        """CDR drug term document."""

        def index(self):
            """Add information about this document to the Elasticsearch DB."""

            opts = dict(
                index=self.loader.index,
                doc_type=self.loader.doctype,
                body=self.data,
                id=str(self.id),
            )
            if not self.data["definition"]:
                args = "skipping %s (no definitions)", self.cdr_id
                self.loader.logger.debug(*args)
                return
            self.loader.es.index(**opts)
            self.index_autocomplete(self.data["term_name"])
            self.index_expand(self.data["term_name"], True)
            for alias in self.data["alias"]:
                self.index_autocomplete(alias["name"])
                if alias["type"] == "US brand name":
                    self.index_expand(alias["name"].lower())

        def index_autocomplete(self, name):
            """Add a node of type autocomplete for one of the term's names.

            Pass:
                name - string for a name of the drug
            """

            opts = dict(
                index=self.loader.index,
                doc_type="autocomplete",
                body=dict(
                    a_term_id=self.id,
                    autocomplete_name=name,
                ),
            )
            self.loader.es.index(**opts)

        def index_expand(self, name, preferred=False):
            """Add a node of type expand for one of the drug term's names.

            Pass:
                name - string for a name of the drug
                preferred - True if this is the term's preferred name
            """

            definitions = self.data["definition"]
            definition = definitions[0] if definitions else None
            first_letter = name[0].lower()
            if first_letter not in ascii_lowercase:
                first_letter = "#"
            opts = dict(
                index=self.loader.index,
                doc_type="expand",
                body=dict(
                    e_term_id=self.id,
                    expand_name=name,
                    first_letter=first_letter,
                    e_definition=definition,
                    e_term_name=name,
                    is_termname=preferred,
                    e_pretty_url_name=self.pretty_url_name,
                ),
            )
            self.loader.es.index(**opts)

        @property
        def data(self):
            """Collect the values for the primary node for the term."""

            if not hasattr(self, "_data"):
                self._data = dict(
                    term_id=self.id,
                    term_name=self.term_name,
                    alias=[],
                    definition=[],
                    related_resources=[],
                    pretty_url_name=self.pretty_url_name,
                )
                nct_id = self.root.get("NCIThesarusConceptID")
                if nct_id:
                    self._data["nci_concept_id"] = [nct_id]
                for node in self.root.findall("OtherName"):
                    alias = dict(
                        name=Doc.get_text(node.find("OtherTermName")),
                        type=Doc.get_text(node.find("OtherNameType")),
                    )
                    self._data["alias"].append(alias)
                for node in self.root.findall("Definition"):
                    text = Doc.get_text(node.find("DefinitionText"))
                    html = self.get_definition_html(node)
                    definition = dict(
                        text=text,
                        html=html,
                    )
                    self._data["definition"].append(definition)
                url = Doc.get_text(self.root.find("CGovInfo/DISUrl"))
                if url:
                    node = self.root.find("CGovInfo/DISTitle")
                    dis_title = Doc.get_text(node)
                    if not dis_title:
                        dis_title = self._data["term_name"]
                    resource = dict(
                        type="DrugSummary",
                        url=url,
                        text=dis_title,
                    )
                    self._data["related_resources"].append(resource)
            return self._data

        @property
        def pretty_url_name(self):
            """NCI/T term name cleaned up for use as a pretty URL."""

            if not hasattr(self, "_pretty_url_name"):
                name = Doc.get_text(self.root.find("CGovInfo/NCITName"))
                if not name:
                    name = self.term_name
                self._pretty_url_name = self.clean(name)
            return self._pretty_url_name

        @property
        def term_name(self):
            """CDR preferred name for the term."""

            if not hasattr(self, "_term_name"):
                self._term_name = Doc.get_text(self.root.find("PreferredName"))
            return self._term_name

        @staticmethod
        def get_definition_html(node):
            """Convert ExternalRefs to links."""

            segments = []
            node = node.find("DefinitionText")
            if node is None:
                return None
            if node.text is not None:
                segments = [node.text]
            for child in node.findall("*"):
                text = Doc.get_text(child, "")
                if child.tag == "ExternalRef":
                    url = child.get("xref")
                    link = builder.A(text, href=url)
                    segments.append(html.tostring(link, encoding="unicode"))
                else:
                    segments.append(text)
                if child.tail is not None:
                    segments.append(child.tail)
            return "".join(segments)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    choices = "drugs", "glossary"
    parser.add_argument("--dictionary", "-d", choices=choices, required=True)
    parser.add_argument("--tier", "-t")
    parser.add_argument("--loglevel", "-l", default="INFO")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--host", "-s")
    parser.add_argument("--port", "-p", type=int)
    parser.add_argument("--recips", "-r")
    opts = vars(parser.parse_args())
    Loader(None, f"Load {opts['dictionary']}", **opts).run()
