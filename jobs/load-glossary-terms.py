#!/usr/bin/env python

from dictionary_loader import DictionaryAPILoader
from lxml import etree
from cdrapi.docs import Doc
from string import ascii_lowercase


class Loader(DictionaryAPILoader):

    TYPE = "glossary"
    ALIAS = "glossaryv1"
    INDEXDEF = "glossary.json"
    HOST = "ncias-d1592-v"
    PORT = 9400

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
                self.loader.logger.warning("%s: no spanish def", self.cdr_id)

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


if __name__ == "__main__":
    loader = Loader()
    try:
        loader.run()
    except Exception:
        loader.logger.exception("failure indexing drug terms")
        raise
