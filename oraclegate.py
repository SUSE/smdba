#
# Oracle administration gate.
#
# Author: Bo Maryniuk <bo@suse.de>
#

from basegate import BaseGate
import os
import sys


class OracleGate(BaseGate):
    """
    Gate for Oracle database tools.
    """
    NAME = "oracle"
    ORATAB = "/etc/oratab"
    LSNR_CTL = "%s/bin/lsnrctl"


    def __init__(self, config):
        """
        Constructor.
        """
        self.config = config

        # Get Oracle home
        if not os.path.exists(self.ORATAB):
            raise Exception("Underlying error: file \"%s\" does not exists or cannot be read." % self.ORATAB)

        dbsid = self.config.get("db_name")
        for tabline in filter(None, [line.strip() for line in open(self.ORATAB).readlines()]):
            sid, home, default_start = tabline.split(":")
            if sid == dbsid:
                self.ora_home = home
                break

        if not self.ora_home:
            raise Exception("Underlying error: cannot find Oracle home")

        # Setup environment
        os.environ['LANG'] = 'en_US.utf-8'
        os.environ['ORACLE_HOME'] = self.ora_home
        os.environ['ORACLE_BASE'] = self.ora_home.split("/oracle/product")[0] + "/oracle"
        os.environ['ORACLE_SID'] = self.config.get("db_name")
        os.environ['TNS_ADMIN'] = self.ora_home + "/network/admin"
        if os.environ.get('PATH', '').find(self.ora_home) < 0:
            os.environ['PATH'] = self.ora_home + ":" + os.environ['PATH']

        # Get lsnrctl
        self.lsnrctl = self.LSNR_CTL % self.ora_home
        if not os.path.exists(self.lsnrctl):
            raise Exception("Underlying error: %s does not exists or cannot be executed." % self.lsnrctl)


    #
    # Exposed operations below
    #

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
        Start the SUSE Manager Database listener.
        """
        print >> sys.stdout, "Starting database...\t",
        sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        if ready:
            print >> sys.stdout, "Failed"
            print >> sys.stderr, "Error: listener already running."
            return

        ready = False
        stdout, stderr = self.syscall(self.lsnrctl, None, None, "start")
        if stdout:
            for line in stdout.split("\n"):
                if line.lower().startswith("uptime"):
                    ready = True
                    break

            print >> sys.stdout, (ready and "done" or "failed")

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_stop(self):
        """
        Stop the SUSE Manager Database listener.
        """
        print >> sys.stdout, "Stopping database...\t",
        sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        if not ready:
            print >> sys.stdout, "Failed"
            print >> sys.stderr, "Error: listener is not running."
            return

        success = False
        stdout, stderr = self.syscall(self.lsnrctl, None, None, "stop")
        if stdout:
            for line in stdout.split("\n"):
                if line.lower().find("completed successfully") > -1:
                    success = True
                    break

            print >> sys.stdout, (success and "done" or "failed")

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_status(self):
        """
        Show database status.
        """
        print >> sys.stdout, "Database listener is",
        sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        print >> sys.stdout, (ready and "running" or "down") + ".", uptime and uptime or ""
        
        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_tablesizes(self):
        """
        Show space report for each table.
        """

    def do_verify(self):
        """
        Verify an SUSE Manager Database Instance backup.
        """

    #
    # Helpers below
    #

    def get_status(self):
        """
        Get Oracle listener status.
        """
        uptime = ""
        ready = False
        sid = self.config.get("db_name", "")
        stdout, stderr = self.syscall(self.lsnrctl, None, None, "status")
        if stdout:
            for line in stdout.split("\n"):
                if line.lower().startswith("uptime"):
                    uptime = "Uptime: " + line.replace("\t", " ").split(" ", 1)[-1].strip()
                    ready = True
                    break
        
        return ready, uptime, stderr




def getGate(config):
    """
    Get gate to the database engine.
    """
    return OracleGate(config)
