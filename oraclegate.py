from basegate import BaseGate


class OracleGate(BaseGate):
    """
    Gate for Oracle database tools.
    """
    NAME = "oracle"

    def do_hot_backup(self):
        """
        Perform database hot backup on running database.
        """


    def do_cold_backup(self):
        """
        Perform database backup.
        """

    def do_backup_info(self):
        """
        Display information about an SUSE Manager Database backup.
        """

    def do_extend(self):
        """
        Increase the SUSE Manager Database Instance tablespace.
        """

    def do_get_stats(self):
        """
        Gather statistics on SUSE Manager Database database objects.
        """

    def do_report(self):
        """
        Show database space report.
        """

    def do_report_stats(self):
        """
        Show tables with stale or empty statistics.
        """

    def do_restore_backup(self):
        """
        Restore the SUSE Manager Database from backup.
        """

    def do_shrink_segments(self):
        """
        Shrink SUSE Manager Database database segments.
        """


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

    def do_tablesizes(self):
        """
        Show space report for each table.
        """

    def do_verify(self):
        """
        Verify an SUSE Manager Database Instance backup.
        """


def getGate():
    """
    Get gate to the database engine.
    """
    return OracleGate()
