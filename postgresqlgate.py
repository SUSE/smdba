from basegate import BaseGate


class PgSQLGate(BaseGate):
    """
    Gate for PostgreSQL database tools.
    """
    NAME = "postgresql"


    def do_start(self):
        """
        Start the SUSE Manager Database.
        """


    def do_stop(self):
        """
        Stop the SUSE Manager Database.
        """


    def do_status(self):
        """
        Show database status.
        """



def getGate():
    """
    Get gate to the database engine.
    """
    return PgSQLGate()
