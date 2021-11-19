"""Load clinical trial info into ElasticSearch.
"""

from argparse import ArgumentParser
from datetime import datetime
from json import dump, dumps, loads, load
from re import compile
from sys import stderr
from time import sleep
from unicodedata import combining, normalize
from elasticsearch7 import Elasticsearch
from requests import get
from .base_job import Job
from cdr import getControlValue
from cdrapi.settings import Tier


class Loader(Job):
    """Top-level control for job.

    Options:
      * auth (optional comma-separated username and password)
      * concepts (locally cached dump of listing info records)
      * debug (increase level of logging)
      * host (override ElasticSearch host name)
      * limit (throttle the number of concepts for testing)
      * sleep (override maximum number of seconds to sleep on failure)
      * port (override ElasticSearch port number)
      * test (write to the file system, not ElasticSearch)
      * verbose (write progress to the console)
      * groups (locally cached dump of listing info records)
    """

    API = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit"
    BAD = {"C138195", "C131913"}
    DISEASE = "C7057"
    INTERVENTION = "C1908"
    TOP = DISEASE, INTERVENTION
    LOGNAME = "trial-info"
    CONTROL_GROUP = "trials"
    LABELS = "labels"
    TOKENS = "tokens"
    OVERRIDES = "overrides"
    INFO_DEF = "listing-info"
    TRIAL_DEF = "trial-type-info"
    INFO = "ListingInfo"
    TRIAL = "TrialTypeInfo"
    INFO_ALIAS = "listinginfov1"
    TRIAL_ALIAS = "trialtypeinfov1"
    LIMIT = 1000000
    MIN_SLEEP = .01
    MAX_SLEEP = 500
    MAX_PRETTY_URL_LENGTH = 75
    BATCH_SIZE = 500
    SUPPORTED_PARAMETERS = {
        "auth",
        "concepts",
        "debug",
        "dump",
        "groups",
        "host",
        "limit",
        "port",
        "sleep",
        "test",
        "verbose",
    }

    def run(self):
        """Generate JSON records for the API from EVS concepts.

        Fetch and parse the concept records, determining the display
        name for each, and collect concepts into groups sharing the
        same normalized display name.

        We append a handful of records representing hand-curated
        mappings of some of the labels.
        """

        if self.opts.get("debug"):
            self.logger.setLevel("DEBUG")
        groups = self.groups
        labels = [label for label in self.labels]
        if self.dump:
            self.__dump(groups, labels)
        if self.testing:
            with open(f"{self.INFO}-{self.stamp}.json", "w") as fp:
                dump({self.INFO: groups}, fp, indent=2)
            with open(f"{self.TRIAL}-{self.stamp}.json", "w") as fp:
                dump({self.TRIAL: labels}, fp, indent=2)
        else:
            self.__index(groups, labels)
        if self.verbose:
            stderr.write("done\n")

    @property
    def auth(self):
        """Username, password tuple."""

        if not hasattr(self, "_auth"):
            self._auth = None
            auth = self.opts.get("auth")
            if auth:
                self._auth = auth.split(",", 1)
        return self._auth

    @property
    def concepts(self):
        """Dictionary of Concept objects, indexed by code.

        Loaded by the `groups` property, which starts with the top-level
        concept records for disease and intervention and fetches the
        concepts recursively.
        """

        if not hasattr(self, "_concepts"):
            self._concepts = dict()
        return self._concepts

    @property
    def es(self):
        """Connection to the ElasticSearch server."""

        if not hasattr(self, "_es"):
            opts = dict(host=self.host, port=self.port, timeout=300)
            if self.auth:
                opts["http_auth"] = self.auth
            self._es = Elasticsearch([opts])
        return self._es

    @property
    def dump(self):
        """If True, write test data to the file system."""
        return True if self.opts.get("dump") else False

    @property
    def groups(self):
        """Sequence of groups of concepts sharing display names."""

        if not hasattr(self, "_groups"):

            # Use a locally cached dump if one is specified.
            groups = self.opts.get("groups")
            if groups:
                self._groups = []
                with open(groups) as fp:
                    for line in fp:
                        values = loads(line.strip())
                        if "concept_id" in values:
                            self._groups.append(values)
                return self._groups

            groups = dict()
            start = datetime.now()
            concepts = self.opts.get("concepts")
            if concepts:
                class CachedConcept:
                    def __init__(self, code, name):
                        self.code = code
                        self.name = name
                        self.key = name.lower()
                with open(concepts) as fp:
                    concepts = [CachedConcept(*values) for values in load(fp)]
            else:
                self.__fetch(self.TOP)
                concepts = self.concepts.values()
                if self.dump:
                    values = [(c.code, c.name) for c in concepts]
                    values = [(int(v[0][1:]), v[0], v[1]) for v in values]
                    values = [v[1:] for v in sorted(values)]
                    with open(f"concepts-{self.stamp}.json", "w") as fp:
                        dump(values, fp, indent=2)
            args = len(concepts), datetime.now() - start
            self.logger.info("fetched %d concepts in %s", *args)
            if self.verbose:
                stderr.write("\n")
            for concept in concepts:
                if concept.code in self.BAD:
                    continue
                group = groups.get(concept.key)
                if not group:
                    group = groups[concept.key] = Group(self, concept)
                    if self.verbose:
                        stderr.write(f"\rfound {len(groups)} groups")
                group.codes.append(concept.code)
                if len(group.codes) > 1:
                    args = len(group.codes), group.key
                    self.logger.info("%d codes for %r", *args)
            self._groups = sorted(groups.values())
            for group in self._groups:
                matches = {}
                for code in group.codes:
                    if code in self.overrides:
                        override = self.overrides[code]
                        codes = str(sorted(override.codes))
                        if codes not in matches:
                            matches[codes] = override
                if len(matches) > 1:
                    matches = " and ".join(matches)
                    error = f"group {group.key} matches overrides {matches}"
                    raise Exception(error)
                if matches:
                    key, override = matches.popitem()
                    if override.matched_by:
                        both = f"{group.key} and {override.matched_by}"
                        error = f"override for {codes} matches {both}"
                        raise Exception(error)
                    override.matched_by = group.key
                    if override.url == Override.BLANK:
                        group.url = None
                    elif override.url != Override.INHERIT:
                        group.url = override.url
                    if override.label != Override.INHERIT:
                        group.name = override.label
                        key = group.name.lower()
                        other_group = groups.get(key)
                        if other_group and other_group is not group:
                            message = f"override results in two {key!r} groups"
                            self.logger.warning(message)
            urls = {}
            for group in self._groups:
                if group.url in urls:
                    other = urls[group.url]
                    message = f"{group.url} used by {group.key} and {other}"
                    raise Exception(message)
            self._groups = [group.values for group in self._groups]
            if self.verbose:
                stderr.write("\n")
        return self._groups

    @property
    def host(self):
        """Name of the ElasticSearch server."""

        if not hasattr(self, "_host"):
            self._host = self.opts.get("host")
            if not self._host:
                self._host = self.tier.hosts.get("DICTIONARY")
            if not self._host:
                raise Exception("no database host specified")
            if self.verbose:
                stderr.write(f"connecting to {self._host}\n")
            self.logger.info("connecting to %s", self._host)
        return self._host

    @property
    def info_def(self):
        """Schema for the listing info records."""

        if not hasattr(self, "_info_def"):
            info_def = getControlValue(self.CONTROL_GROUP, self.INFO_DEF)
            self._info_def = loads(info_def)
        return self._info_def

    @property
    def labels(self):
        """Map tuples to dictionaries."""

        if not hasattr(self, "_labels"):
            self._labels = []
            labels = getControlValue(self.CONTROL_GROUP, self.LABELS)
            for line in labels.splitlines():
                values = line.strip().split("|")
                url, id, label = [value.strip() for value in values]
                self._labels.append(dict(
                    pretty_url_name=url,
                    id_string=id.strip(),
                    label=label.strip(),
                ))
        return self._labels

    @property
    def limit(self):
        """Throttle the number of concepts for testing."""

        if not hasattr(self, "_limit"):
            self._limit = int(self.opts.get("limit") or self.LIMIT)
        return self._limit

    @property
    def max_sleep(self):
        """Longest we will wait between fetch failures."""

        if not hasattr(self, "_max_sleep"):
            seconds = float(self.opts.get("sleep") or self.MAX_SLEEP)
            self._max_sleep = max(seconds, self.MIN_SLEEP)
        return self._max_sleep

    @property
    def overrides(self):
        """Hand-crafted labels and pretty URLs."""

        if not hasattr(self, "_overrides"):
            overrides = getControlValue(self.CONTROL_GROUP, self.OVERRIDES)
            self._overrides = {}
            urls = {}
            for line in overrides.splitlines():
                override = Override(line)
                if override.url in urls:
                    message = f"URL {override.url} in multiple overrides"
                    raise Exception(message)
                for code in override.codes:
                    if not code:
                        raise Exception(f"empty code in {line}")
                    if code in self._overrides:
                        message = f"code {code} in multiple overrides"
                        raise Exception(message)
                    self._overrides[code] = override
        return self._overrides

    @property
    def port(self):
        """TCP/IP port on which we connect."""

        if not hasattr(self, "_port"):
            port = self.opts.get("port")
            if not port:
                port = self.tier.ports.get("dictionary")
            if not port:
                raise Exception("no database port specified")
            try:
                self._port = int(port)
            except:
                raise Exception("invalid port value")
            if self.verbose:
                stderr.write(f"connecting on port {self._port}\n")
            self.logger.info("connecting on port %d", self._port)
        return self._port

    @property
    def stamp(self):
        """Date/time string used to create unique names."""

        if not hasattr(self, "_stamp"):
            self._stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return self._stamp

    @property
    def testing(self):
        """If True, write to file system instead of ElasticSearch."""
        return True if self.opts.get("test") else False

    @property
    def tier(self):
        """Which CDR tier are we using?"""

        if not hasattr(self, "_tier"):
            self._tier = Tier()
        return self._tier

    @property
    def tokens(self):
        """Strings which we don't alter when we normalize display names."""

        if not hasattr(self, "_tokens"):
            self._tokens = set()
            tokens = getControlValue(self.CONTROL_GROUP, self.TOKENS)
            for line in tokens.splitlines():
                self._tokens.add(line.strip())
        return self._tokens

    @property
    def trial_def(self):
        """Schema for the listing trial records."""

        if not hasattr(self, "_trial_def"):
            trial_def = getControlValue(self.CONTROL_GROUP, self.TRIAL_DEF)
            self._trial_def = loads(trial_def)
        return self._trial_def

    @property
    def verbose(self):
        """Show progress (for running from the command line)."""

        if not hasattr(self, "_verbose"):
            self._verbose = True if self.opts.get("verbose") else False
        return self._verbose

    def __create_alias(self, index, alias):
        """Point the canonical name for the records to our new index."""

        actions = []
        if self.es.indices.exists_alias(name=alias):
            aliases = self.es.indices.get_alias(alias)
            for old_index in aliases:
                if "aliases" in aliases[old_index]:
                    if alias in aliases[old_index]["aliases"]:
                        actions.append(dict(
                            remove=dict(
                                index=old_index,
                                alias=alias,
                            )
                        ))
        actions.append(dict(add=dict(index=index, alias=alias)))
        # stderr.write(f"actions: {actions}\n")
        self.es.indices.update_aliases(body=dict(actions=actions))

    def __dump(self, groups, labels):
        """Create dump files for API testing.

        Pass:
          groups - sequence of concept groups for ListingInfo records
          labels - sequence of value dictionaries for TrialTypeInformation
        """

        action = dict(index=dict(_index=self.INFO_ALIAS))
        action = dumps(action)
        with open(f"{self.INFO}.dump", "w") as fp:
            for group in groups:
                fp.write(f"{action}\n")
                fp.write(f"{dumps(group)}\n")
        action = dict(index=dict(_index=self.TRIAL_ALIAS))
        action = dumps(action)
        with open(f"{self.TRIAL}.dump", "w") as fp:
            for label in labels:
                fp.write(f"{action}\n")
                fp.write(f"{dumps(label)}\n")

    def __fetch(self, codes):
        """Fetch a concept and its children recursively.

        Populates the `concepts` property as a side effect.

        We attempt repeatedly to fetch the concept until we succeed
        or run out of patience and conclude that the EVS is ailing.
        Note that we inject a tiny bit of sleep between each fetch
        even when there are no failures, to increase the chances that
        the EVS will be able to keep up. :-)

        Pass:
            codes - sequence of strings for the unique concept IDs in the EVS
        """

        seconds = self.MIN_SLEEP
        url = f"{self.API}?include=full&list={','.join(codes)}"
        while True:
            sleep(seconds)
            try:
                response = get(url, timeout=5)
                concepts = response.json()
                break
            except Exception:
                self.logger.exception(url)
                seconds *= 2
                if seconds > self.max_sleep:
                    self.logger.error("EVS has died -- bailing")
                    raise Exception("EVS has died")
        if len(codes) != len(concepts):
            self.logger.warning("got %d concepts for %r", len(concepts), codes)
            self.logger.warning(response.text)
        for values in concepts:
            concept = Concept(values)
            self.concepts[concept.code] = concept
            if self.verbose:
                stderr.write(f"\rfetched {len(self.concepts)} concepts")
            self.logger.debug("fetched %s", concept.code)
        children = set()
        for values in concepts:
            if len(self.concepts) >= self.limit:
                break
            for child in values.get("children", []):
                if len(self.concepts) + len(children) >= self.limit:
                    break
                code = child.get("code", "").upper()
                if code and code not in self.concepts:
                    children.add(code)
        if children:
            i = 0
            children = list(children)
            while i < len(children):
                self.__fetch(children[i:i+self.BATCH_SIZE])
                i += self.BATCH_SIZE

    def __index(self, groups, labels):
        """Create ElasticSearch indexes, load them, and alias them.

        Pass:
          groups - sequence of concept groups for ListingInfo records
          labels - sequence of value dictionaries for TrialTypeInformation
        """

        start = datetime.now()
        info_index = f"{self.INFO_ALIAS}-{self.stamp}"
        trial_index = f"{self.TRIAL_ALIAS}-{self.stamp}"
        if self.verbose:
            stderr.write(f"creating {info_index}\n")
        self.es.indices.create(index=info_index, body=self.info_def)
        if self.verbose:
            stderr.write(f"creating {trial_index}\n")
        self.es.indices.create(index=trial_index, body=self.trial_def)
        if self.verbose:
            stderr.write("indexes created\n")
        opts = dict(index=info_index) #, doc_type=self.INFO)
        if self.verbose:
            done = 0
        for group in groups:
            self.logger.debug("indexing %s", group["concept_id"])
            opts["body"] = group
            try:
                self.es.index(**opts)
            except Exception as e:
                stderr.write(f"\n{group}\n")
                raise
            if self.verbose:
                done += 1
                stderr.write(f"\r{done} of {len(groups)} groups indexed")
        if self.verbose:
            stderr.write(f"\n{info_index} populated\n")
            done = 0
        opts = dict(index=trial_index)
        for label in labels:
            self.logger.debug("indexing label %s", label["id_string"])
            opts["body"] = label
            try:
                self.es.index(**opts)
            except Exception as e:
                stderr.write(f"\n{label}\n")
                raise
            if self.verbose:
                done += 1
                stderr.write(f"\r{done} of {len(labels)} labels indexed")
        if self.verbose:
            if labels:
                stderr.write("\n")
            stderr.write(f"{trial_index} populated\n")
        opts = dict(max_num_segments=1, index=info_index)
        self.es.indices.forcemerge(**opts)
        opts["index"] = trial_index
        self.es.indices.forcemerge(**opts)
        if self.verbose:
            stderr.write("indexes merged\n")
        if not self.opts.get("limit"):
            self.__create_alias(info_index, self.INFO_ALIAS)
            self.__create_alias(trial_index, self.TRIAL_ALIAS)
            if self.verbose:
                stderr.write("index aliases updated\n")
        self.logger.info("indexing completed in %s", datetime.now() - start)


class Override:
    """Replacement values for a group of concepts."""

    BLANK = "<BLANK>"
    INHERIT = "<INHERIT>"
    URL_PATTERN = compile("^[a-z0-9-]+$")

    def __init__(self, line):
        """Parse the replacement record.

        Pass:
          line - override values in the form CODE[,CODE[,...]]|LABEL|URL
        """

        self.matched_by = None
        line = line.strip()
        fields = line.split("|")
        if len(fields) != 3:
            raise Exception(f"malformed override {line}")
        codes, label, url = fields
        codes = codes.split(",")
        self.codes = {code.strip().upper() for code in codes}
        if not self.codes:
            raise Exception(f"missing codes in {line}")
        for code in self.codes:
            if not code:
                raise Exception(f"empty code in {line.strip()}")
        self.label = label.strip()
        if not self.label:
            raise Exception(f"empty label in {line}")
        self.url = url.strip()
        if not self.url:
            raise Exception(f"empty URL in {line}")
        if not self.URL_PATTERN.match(self.url):
            if self.url not in (self.BLANK, self.INHERIT):
                raise Exception(f"invalid override URL {url!r}")
        if len(self.url) > Loader.MAX_PRETTY_URL_LENGTH:
            raise Exception("override URL {url} is too long")


class Concept:
    """Values for a single concept in the EVS."""

    SPACES = compile(r"\s+")
    NONE = "[NO DISPLAY NAME]"

    def __init__(self, values):
        """Pull out the pieces we need and discard the rest.

        Pass:
            values - dictionary of concept values pulled from EVS JSON
        """

        display_name = preferred_name = ctrp_name = None
        try:
            synonyms = values.get("synonyms", [])
        except Exception as e:
            stderr.write(f"\n{values}: {e}")
            exit(1)
        for synonym in synonyms:
            if synonym.get("source") == "CTRP":
                if synonym.get("termGroup") == "DN":
                    ctrp_name = (synonym.get("name") or "").strip()
                    if ctrp_name:
                        break
            else:
                name_type = synonym.get("type")
                if name_type == "Preferred_Name":
                    preferred_name = (synonym.get("name") or "").strip()
                elif name_type == "Display_Name":
                    display_name = (synonym.get("name") or "").strip()
        name = ctrp_name or display_name or preferred_name or self.NONE
        self.name = self.SPACES.sub(" ", name)
        self.key = self.name.lower()
        self.code = values["code"].upper()


class Group:
    """Set of concepts sharing a common normalized display string."""

    _FROM = "\u03b1\u03b2\u03bc;_&\u2013/"
    _TO = "abu-----"
    _STRIP = "\",+().\xaa'\u2019[\uff1a:*\\]"
    TRANS = str.maketrans(_FROM, _TO, _STRIP)
    NON_DIGITS = compile("[^0-9]+")

    def __init__(self, loader, concept):
        """Pull what we need from the caller's Concept object.

        Pass:
            loader - access to logger and tokens
            concept - object with the group's common display name
        """

        self.logger = loader.logger
        self.preserve = loader.tokens
        self.name = concept.name
        self.key = concept.key
        self.codes = []

    def __lt__(self, other):
        """Sort the groups by key."""
        return self.key < other.key

    @property
    def phrase(self):
        """Display name mostly lowercased for use in running text.

        Lowercase each word in the name except for those appearing
        in self.preserve.
        """

        if not hasattr(self, "_phrase"):
            words = []
            for word in self.name.split():
                if word not in self.preserve:
                    word = word.lower()
                words.append(word)
            self._phrase = " ".join(words)
        return self._phrase

    @property
    def values(self):
        """Dictionary of values to be serialized as JSON for the group."""

        if not hasattr(self, "_values"):
            codes = []
            for code in self.codes:
                codes.append(int(self.NON_DIGITS.sub("", code)))
            self._values = dict(
                concept_id = [f"C{code:d}" for code in sorted(codes)],
                name=dict(
                    label=self.name,
                    normalized=self.phrase,
                ),
                pretty_url_name=self.url,
            )
        return self._values

    @property
    def url(self):
        """Prepare the group's display name for use as a pretty URL."""

        if not hasattr(self, "_url"):
            name = self.key.replace(" ", "-").translate(self.TRANS)
            nfkd = normalize("NFKD", name)
            url = "".join([c for c in nfkd if not combining(c)])
            self._url = url.replace("%", "pct")
            if len(self._url) > Loader.MAX_PRETTY_URL_LENGTH:
                args = self._url, self.key
                self.logger.warning("dropping overlong url %r for %r", *args)
                self._url = None
        return self._url

    @url.setter
    def url(self, value):
        """Apply replacement URL from override set.

        Pass:
            value - new value
        """

        self._url = value


if __name__ == "__main__":
    """Don't launch the script if loaded as a module."""

    parser = ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="do more logging")
    parser.add_argument("--dump", "-d", action="store_true",
                        help="save files for testing the API")
    parser.add_argument("--host", help="ElasticSearch server")
    parser.add_argument("--limit", type=float,
                        help="maximum concepts to fetch from the EVS")
    parser.add_argument("--sleep", type=int, metavar="SECONDS",
                        help="longest delay between fetch failures")
    parser.add_argument("--port", type=int,
                        help="ElasticSearch port (default 9400)")
    parser.add_argument("--test", action="store_true",
                        help="save to file system, not ElasticSearch")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="show progress on the command line")
    parser.add_argument("--groups", "-g",
                        help="dump file name for listing info records")
    parser.add_argument("--concepts", "-c", help="dump of concept values")
    parser.add_argument("--auth", "-a",
                        help="comma-separated username/password")
    opts = parser.parse_args()
    Loader(None, "Load dynamic trial info", **vars(opts)).run()
