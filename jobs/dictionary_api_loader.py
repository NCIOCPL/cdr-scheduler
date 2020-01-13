"""Load dictionary nodes into ElasticSearch.
"""

from datetime import datetime
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


if __name__ == "__main__":
    """Support testing from the command line."""

    import argparse
    parser = argparse.ArgumentParser()
    choices = "drugs", "glossary"
    parser.add_argument("--dictionary", "-d", choices=choices, required=True)
    parser.add_argument("--ids", "-i", type=int, nargs="*")
    parser.add_argument("--tier", "-t")
    parser.add_argument("--loglevel", "-l", default="INFO")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--host", "-s")
    parser.add_argument("--port", "-p", type=int)
    parser.add_argument("--recips", "-r")
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--limit", type=int)
    opts = vars(parser.parse_args())
    Loader(None, f"Load {opts['dictionary']}", **opts).run()
