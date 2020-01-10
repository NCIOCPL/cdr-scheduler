#!/usr/bin/env python

from dictionary_loader import DictionaryAPILoader
from lxml import etree, html
from lxml.html import builder
from cdrapi.docs import Doc
from string import ascii_lowercase


class Loader(DictionaryAPILoader):

    TYPE = "drug"
    ALIAS = "drugv1"
    INDEXDEF = "drugs.json"
    HOST = "ncias-d1592-v"
    PORT = 9400

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
                self.loader.logger.warning(*args)
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
    loader = Loader()
    try:
        loader.run()
    except:
        loader.logger.exception("failure indexing drug terms")
        raise
