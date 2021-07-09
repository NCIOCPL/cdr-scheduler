"""Generate a sitemap and post it to Akamai.

Requirements at https://tracker.nci.nih.gov/browse/OCECDR-4854.
"""

from argparse import ArgumentParser
from datetime import datetime
from os import unlink
from lxml import etree
from paramiko import SSHClient, AutoAddPolicy, RSAKey
from cdr import getControlValue
from cdrapi import db
from dictionary_loader import DictionaryAPILoader
from .base_job import Job


class Loader(Job):

    LOGNAME = "sitemap-loader"
    DESTINATION = "sitemaps/dictionaries.xml"
    NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
    NSMAP = {None: NS}
    NS = f"{{{NS}}}"
    PATTERNS = dict(
        term=dict(
            English="publications/dictionaries/cancer-terms",
            Spanish="espanol/publicaciones/diccionarios/diccionario-cancer",
        ),
        genetic=dict(
            English="publications/dictionaries/genetics-dictionary",
            Spanish="espanol/publicaciones/diccionarios/diccionario-genetica",
        ),
        drug=dict(
            English="publications/dictionaries/cancer-drug"
        ),
    )
    SUPPORTED_PARAMETERS = {"tier", "hostname", "username", "keep", "dump"}

    def run(self):
        """Create the sitemap document and store it on Akamai's servers.

        We maintain a dictionary of glossary names so we only have to
        fetch and parse a GlossaryTerm document once, instead of once
        for the English name and a second time for the Spanish name.

        The selection process is driven by a CSV file, stored in the
        ctl table, with CDR ID, key, and language on each line. Key is
        one of term, genetic, or drug. Language is English or Spanish.
        See the Jira ticket (URL at the top of this file) for more details.
        """

        # Load the entries file.
        tier = self.tier
        self.logger.info("loading sitemap info from %s", tier)
        entries = getControlValue("dictionary", "sitemap-entries", tier=tier)

        # Prepare the database query used to fetch a CDR XML document.
        cursor = db.connect(user="CdrGuest", tier=tier).cursor()
        query = db.Query("pub_proc_cg c", "t.name", "c.xml")
        query.join("document d", "d.id = c.id")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where(query.Condition("c.id", 0))
        query = str(query)

        # Walk through each entry to determine whether we have a URL.
        glossary_names = dict()
        urls = dict()
        for line in entries.splitlines():
            line = line.strip()
            id, key, lang = line.split(",")
            id = int(id)
            cursor.execute(query, (id,))
            row = cursor.fetchone()
            if not row:
                args = lang, key, id
                self.logger.warning("%s %s entry CDR%s not published", *args)
            else:
                if id in glossary_names:
                    node = glossary_names[id].get(lang)
                else:
                    root = etree.fromstring(row.xml.encode("utf-8"))
                    if key == "drug":
                        if row.name != "Term":
                            raise Exception(f"CDR{id} has doctype {row.name}")
                        node = root.find("CGovInfo/NCITName")
                        if node is None or not node.text:
                            node = root.find("PreferredName")
                    else:
                        glossary_names[id] = dict()
                        node = root.find("TermName")
                        glossary_names[id]["English"] = node
                        node = root.find("SpanishTermName")
                        glossary_names[id]["Spanish"] = node
                        node = glossary_names[id].get(lang)
                url = None
                if node is not None and node.text:
                    url = DictionaryAPILoader.Doc.Node.clean_pretty_url(node)
                if not url:
                    args = lang, key, id
                    message = "%s %s entry CDR%s has no URL; using CDR ID"
                    self.logger.warning(message, *args)
                    url = id
                pattern = self.PATTERNS[key][lang]
                url = f"https://www.cancer.gov/{pattern}/def/{url}"
                if url not in urls:
                    urls[url] = []
                urls[url].append((lang, key, id))

        # Build the sitemap document, logging and skipping duplicate URLs.
        urlset = etree.Element(f"{self.NS}urlset", nsmap=self.NSMAP)
        for url in sorted(urls):
            if len(urls[url]) > 1:
                self.logger.warning("duplicate URL %r", url)
                for args in urls[url]:
                    self.logger.warning("... used by %s %s entry CDR%s", *args)
            else:
                node = etree.SubElement(urlset, f"{self.NS}url")
                etree.SubElement(node, f"{self.NS}loc").text = url
                etree.SubElement(node, f"{self.NS}priority").text = "0.5"
                etree.SubElement(node, f"{self.NS}changefreq").text = "weekly"
        xml = etree.tostring(urlset, pretty_print=True, encoding="utf-8")
        if self.opts.get("dump"):
            print(xml.decode("utf-8"))
        else:
            try:
                stamp = datetime.now().strftime("%Y%m%d%H%M%S")
                tempname = f"d:/tmp/sitemap-{stamp}.xml"
                with open(tempname, "wb") as fp:
                    fp.write(xml)
            except Exception as e:
                self.logger.exception("saving sitemap")
                raise
            try:
                with self.client.open_sftp() as sftp:
                    sftp.put(tempname, self.DESTINATION)
                self.logger.info("sent %s to %s", tempname, self.hostname)
                if not self.opts.get("keep"):
                    unlink(tempname)
                else:
                    self.logger.info("preserving %s", tempname)
            except Exception as e:
                self.logger.exception("sending %s", tempname)
                raise

    @property
    def client(self):
        """SSH client for pushing to Akamai."""

        if not hasattr(self, "_client"):
            self._client = SSHClient()
            policy = AutoAddPolicy()
            self._client.set_missing_host_key_policy(policy)
            key = RSAKey.from_private_key_file(self.keyfile)
            opts = dict(
                hostname=self.hostname,
                username=self.username,
                pkey=key
            )
            self._client.connect(**opts)
        return self._client

    @property
    def hostname(self):
        """Which Akamai server are we pushing to?"""

        if not hasattr(self, "_hostname"):
            self._hostname = self.opts.get("hostname")
            if not self._hostname:
                raise Exception("Missing require hostname")
        return self._hostname

    @property
    def keyfile(self):
        """Which ssh key file should we use?"""

        if not hasattr(self, "_keyfile"):
            suffix = ""
            if "-dev" in self.hostname:
                suffix = "-dev"
            elif "-stage" in self.hostname:
                suffix = "-test"
            self._keyfile = f"d:/etc/akamai-sitemap{suffix}"
        return self._keyfile

    @property
    def tier(self):
        """Where are we getting our data from?"""

        if not hasattr(self, "_tier"):
            self._tier = self.opts.get("tier", "PROD")
        return self._tier

    @property
    def username(self):
        """SSH account name for logging into Akamai."""

        if not hasattr(self, "_username"):
            self._username = self.opts.get("username")
            if not self._username:
                raise Exception("Missing require username")
        return self._username


if __name__ == "__main__":
    """Don't execute script if loaded as a module."""

    parser = ArgumentParser()
    parser.add_argument("--tier")
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--dump", help="print instead of store sitemap")
    opts = vars(parser.parse_args())
    Loader(None, "Sitemap Loader", **opts).run()
