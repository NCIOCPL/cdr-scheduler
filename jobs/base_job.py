"""Base class whose derived classes implement job functionality.
"""

import logging
from threading import Lock
from cdr import DEFAULT_LOGDIR
from cdrapi import db


class Job:
    """Override to implement a scheduled job class.

    To enforce passing only supported parameters for a job type, set the
    class-level SUPPORTED_PARAMETERS to a set of parameter names.
    """

    import lxml.html.builder as B
    import lxml.html as HTML
    LOGNAME = "scheduled-job"
    LOGGING_LOCK = Lock()
    LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
    SENDER = "PDQ Operator <NCIPDQoperator@mail.nih.gov>"
    CHARSET = "utf-8"
    TSTYLE = (
        "width: 95%",
        "border: 1px solid #999",
        "border-collapse: collapse",
        "margin-top: 30px"
    )
    TSTYLE = "; ".join(TSTYLE)
    TO_STRING_OPTS = {
        "pretty_print": True,
        "encoding": "unicode",
        "doctype": "<!DOCTYPE html>"
    }
    SUPPORTED_PARAMETERS = None

    def __init__(self, control, name, **opts):
        self.control = control
        self.name = name
        self.opts = opts
        if self.opts and self.SUPPORTED_PARAMETERS is not None:
            unsupported = set(self.opts) - set(self.SUPPORTED_PARAMETERS)
            if unsupported:
                unsupported = sorted(unsupported)
                if len(unsupported) == 1:
                    msg = f"unsupported parameter {unsupported[0]}"
                else:
                    unsupported = ", ".join(unsupported)
                    msg = "unsupported parameters: {unsupported}"
                raise Exception(msg)

    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            with Job.LOGGING_LOCK:
                self._logger = logging.getLogger(self.LOGNAME)
                if not (self._logger.handlers):
                    self._logger.setLevel(logging.INFO)
                    path = f"{DEFAULT_LOGDIR}/{self.LOGNAME}.log"
                    handler = logging.FileHandler(path)
                    formatter = logging.Formatter(self.LOG_FORMAT)
                    handler.setFormatter(formatter)
                    self._logger.addHandler(handler)
        return self._logger

    def run(self):
        raise Exception("derived class must override run() method")

    @staticmethod
    def get_group_email_addresses(group_name="Developers Notification"):
        """
        Replacement for cdr.getEmailList() which does not exclude retired
        accounts.
        """
        query = db.Query("usr u", "u.email")
        query.join("grp_usr gu", "gu.usr = u.id")
        query.join("grp g", "g.id = gu.grp")
        query.where(query.Condition("g.name", group_name))
        query.where("u.expired IS NULL")
        return [row[0] for row in query.execute().fetchall() if row[0]]

    @classmethod
    def th(cls, label, **styles):
        """
        Helper method to generate a table column header.

        label      Display string for the column header
        styles     Optional style tweaks. See merge_styles() method.
        """

        default_styles = {
            "font-family": "Arial",
            "border": "1px solid #999",
            "margin": "auto",
            "padding": "2px",
        }
        style = cls.merge_styles(default_styles, **styles)
        return cls.B.TH(label, style=style)

    @classmethod
    def td(cls, data, url=None, **styles):
        """
        Helper method to generate a table data cell.

        data       Data string to be displayed in the cell
        styles     Optional style tweaks. See merge_styles() method.
        """

        default_styles = {
            "font-family": "Arial",
            "border": "1px solid #999",
            "vertical-align": "top",
            "padding": "2px",
            "margin": "auto"
        }
        style = cls.merge_styles(default_styles, **styles)
        if url:
            return cls.B.TD(cls.B.A(data, href=url), style=style)
        return cls.B.TD(data, style=style)

    @classmethod
    def serialize(cls, html):
        """
        Create a properly encoded string for the report.

        html       Tree object created using lxml HTML builder.
        """

        return cls.HTML.tostring(html, **cls.TO_STRING_OPTS)

    @staticmethod
    def merge_styles(defaults, **styles):
        """
        Allow the default styles for an element to be overridden.

        defaults   Dictionary of style settings for a given element.
        styles     Dictionary of additional or replacement style
                   settings. If passed as separate arguments the
                   setting names with hyphens will have to have been
                   given with underscores instead of hyphens. We
                   restore the names which CSS expects.
        """

        d = dict(defaults, **styles)
        s = ["%s:%s" % (k.replace("_", "-"), v) for k, v in d.items()]
        return ";".join(s)
