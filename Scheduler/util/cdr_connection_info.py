# 
import cdrpw, cdrutil


class CDRDBConnectionInfo:
    """
    Utility code to encapsulate look up for host/tier specific database connection
    information. This is a wrapper around the core cdrpw and cdrutil modules, using
    logic copied from the cdrdb module.
    
    All properties receive values upon instantiation. There are no public methods.
    
    Properties exposed:
        Server   - DNS name of the database server
        Port     - port number to connect to
        Database - name of the database to connect to
        Tier     - The tier the system is set up on.
        Password - login password
        Username - login userid
    """

    # We always want to use cdrsqlaccount and connect to the CDR database
    _USERNAME = 'cdrsqlaccount'
    _DATABASE = 'cdr'

    def __init__(self):
        self.Server = None
        self.Port = 0
        self.Database = CDRDBConnectionInfo._DATABASE
        self.Tier =  None
        self.Password = None
        self.Username = CDRDBConnectionInfo._USERNAME
        
        hostInfo = cdrutil.AppHost(cdrutil.getEnvironment(), cdrutil.getTier())
        self.Server = CDRDBConnectionInfo._getDBServer(hostInfo)
        self.Port = CDRDBConnectionInfo._getDBPort(hostInfo)
        self.Tier =  CDRDBConnectionInfo._getTier(hostInfo)
        self.Password = CDRDBConnectionInfo._getPassword(hostInfo)
    
    @staticmethod
    def _getDBServer(hostInfo):
        return hostInfo.host['DBWIN'][0]
    
    @staticmethod
    def _getDBPort(hostInfo):
        # hack for looking up port number, copied from cdrdb.connect() method.
        port = 52400
        if hostInfo.tier == "PROD":
            port = 55733
        elif hostInfo.tier == "STAGE":
            port = 55459
        elif hostInfo.tier == "QA":
            port = 53100

        return port

    @staticmethod
    def _getTier(hostInfo):
        return hostInfo.tier

    @staticmethod
    def _getPassword(hostInfo):
        return cdrpw.password('CBIIT',hostInfo.tier,CDRDBConnectionInfo._DATABASE,CDRDBConnectionInfo._USERNAME)
        
if __name__ == "__main__":
    x = CDRDBConnectionInfo();
    print " Server: %s\n port: %s\n username: %s\n password: %s\n tier: %s" % \
        (x.DBServer,
         x.DBPort, 
         x.Username,
         x.Password,
         x.Tier)

