"""CDR database connection information."""

from cdrapi.settings import Tier as CDRTier


class CDRDBConnectionInfo:

    """Utility code to encapsulate look up for host/tier specific database
    connection information. This is a wrapper around the core cdrpw
    and cdrutil modules, using logic copied from the cdrdb module.

    All properties receive values upon instantiation. There are no
    public methods.

    Properties exposed:
        Database - name of the database to connect to
        Password - login password
        Port     - port number to connect to
        Server   - DNS name of the database server
        Tier     - The tier the system is set up on.
        Username - login userid
    """

    # We always want to use cdrsqlaccount and connect to the CDR database
    _USERNAME = 'cdrsqlaccount'
    _DATABASE = 'cdr'

    def __init__(self):
        """Create a Tier object, which gets us everything we need."""
        self.__tier = CDRTier()

    @property
    def Database(self):
        return self._DATABASE

    @property
    def Password(self):
        if not hasattr(self, "_Password"):
            self._Password = self.__tier.password(self.Username, self.Database)
        return self._Password

    @property
    def Port(self):
        if not hasattr(self, "_Port"):
            self._Port = self.__tier.port(self.Database)
        return self._Port

    @property
    def Server(self):
        if not hasattr(self, "_Server"):
            self._Server = self.__tier.sql_server
        return self._Server

    @property
    def Tier(self):
        return self.__tier.name

    @property
    def Username(self):
        return self._USERNAME

    def __str__(self):
        return f"""\
Database: {self.Database}
Password: {self.Password}
    Port: {self.Port}
  Server: {self.Server}
    Tier: {self.Tier}
Username: {self.Username}"""

if __name__ == "__main__":
    print(CDRDBConnectionInfo())
