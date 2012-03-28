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
            os.environ['PATH'] = self.ora_home + "/bin:" + os.environ['PATH']

        # Get lsnrctl
        self.lsnrctl = self.LSNR_CTL % self.ora_home
        if not os.path.exists(self.lsnrctl):
            raise Exception("Underlying error: %s does not exists or cannot be executed." % self.lsnrctl)


    def get_scenario_template(self):
        """
        Generate a template for the Oracle SQL*Plus scenario.
        """        
        e = os.environ.get
        scenario = []

        if e('PATH') and e('ORACLE_BASE') and e('ORACLE_SID') and e('ORACLE_HOME'):
            scenario.append("export ORACLE_BASE=" + e('ORACLE_BASE'))
            scenario.append("export ORACLE_SID=" + e('ORACLE_SID'))
            scenario.append("export ORACLE_HOME=" + e('ORACLE_HOME'))
            scenario.append("export PATH=" + e('PATH'))
        else:
            raise Exception("Underlying error: environment cannot be constructed.")

        scenario.append("cat - << EOF | " + e('ORACLE_HOME') + "/bin/sqlplus -S /nolog")
        scenario.append("CONNECT / AS SYSDBA;")
        scenario.append("{scenario}")
        scenario.append("EXIT;")
        scenario.append("EOF")

        return '\n'.join(scenario)


    def call_scenario(self, scenario, **variables):
        """
        Call scenario in SQL*Plus.
        Returns stdout and stderr.
        """
        if not os.path.exists(scenario):
            raise Exception("Underlying error: Scenario {scenario} does not exists or is unreachable.".format(scenario=scenario))
        template = self.get_scenario_template().format(scenario=(open(scenario).read()).replace('$', '\$'))
        if variables:
            template = template.format(**variables)

        return self.syscall("sudo", template, None, "-u", "oracle", "/bin/bash")

    #
    # Exposed operations below
    #

    def _do_hot_backup(self):
        """
        Perform database hot backup on running database.
        """


    def _do_cold_backup(self):
        """
        Perform database backup.
        """

    def _do_backup_info(self):
        """
        Display information about an SUSE Manager Database backup.
        """

    def _do_extend(self):
        """
        Increase the SUSE Manager Database Instance tablespace.
        """

    def do_get_stats(self):
        """
        Gather statistics on SUSE Manager Database database objects.
        """
        stdout, stderr = self.call_scenario('stats.scn', owner=self.config.get('db_user', '').upper())

        stale = []
        empty = []
        if stdout:
            segment = None
            for line in stdout.strip().split("\n"):
                if line.find('stale objects') > -1:
                    segment = 'stale'
                    continue
                elif line.find('empty objects') > -1:
                    segment = 'empty'
                    continue

                line = line.split(" ")[-1].strip()

                if segment and segment == 'stale':
                    stale.append(line)
                elif segment and segment == 'empty':
                    empty.append(line)
                else:
                    print "Ignoring", repr(line)

        if stale:
            print >> sys.stdout, "\nList of stale objects:"
            for obj in stale:
                print >> sys.stdout, "\t", obj
            print >> sys.stdout, "\nFound %s stale objects\n" % len(stale)
        else:
            print >> sys.stdout, "No stale objects found"

        if empty:
            print >> sys.stdout, "\nList of empty objects:"
            for obj in empty:
                print >> sys.stdout, "\t", obj
            print >> sys.stdout, "\nFound %s objects that currently have no statistics.\n" % len(stale)
        else:
            print >> sys.stdout, "No empty objects found."

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_report(self):
        """
        Show database space report.
        """
        stdout, stderr = self.call_scenario('report.scn')
        nw = 10
        sw = 9
        uw = 9
        aw = 10
        ew = 5
        index = [("Tablespace", "Size (Mb)", "Used (Mb)", "Avail (Mb)", "Use %"),]
        for name, free, used, size in [" ".join(filter(None, line.replace("\t", " ").split(" "))).split(" ") 
                                       for line in stdout.strip().split("\n")[2:]]:
            usage = str(int(float(used) / float(size) * 100))
            index.append((name, free, used, size, usage,))
            nw = len(name) > nw and len(name) or nw
            sw = len(size) > sw and len(size) or sw
            uw = len(used) > uw and len(used) or uw
            aw = len(free) > aw and len(free) or aw
            ew = len(usage) > ew and len(usage) or ew

        print >> sys.stdout, "%s\t\t%s\t%s\t%s\t%s" % tuple(index[0])
        for name, free, used, size, usage in index[1:]:
            print >> sys.stdout, "%s\t\t%s\t%s\t%s\t%s" % (name + ((nw - len(name)) * " "),
                                                                    free + ((aw - len(free)) * " "),
                                                                    used + ((uw - len(used)) * " "),
                                                                    size + ((sw - len(size)) * " "),
                                                                    usage + ((ew - len(usage)) * " "))


    def _do_report_stats(self):
        """
        Show tables with stale or empty statistics.
        """

    def _do_restore_backup(self):
        """
        Restore the SUSE Manager Database from backup.
        """

    def _do_shrink_segments(self):
        """
        Shrink SUSE Manager Database database segments.
        """


    def do_listener_start(self):
        """
        Start the SUSE Manager Database listener.
        """
        print >> sys.stdout, "Starting database listener...\t",
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


    def do_listener_stop(self):
        """
        Stop the SUSE Manager Database listener.
        """
        print >> sys.stdout, "Stopping database listener...\t",
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


    def do_listener_status(self):
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


    def do_listener_restart(self):
        """
        Restart SUSE Database Listener.
        """
        print >> sys.stdout, "Restarting listener...",
        sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        if ready:
            self.do_listener_stop()

        self.do_listener_start()

        print >> sys.stdout, "done"


    def do_db_start(self):
        """
        Start SUSE Manager database.
        """
        print >> sys.stdout, "Starting the SUSE Manager database...\t",
        sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        if ready:
            print >> sys.stdout, "Failed"
            print >> sys.stderr, "Error: SUSE Manager database is already running"
            return
        else:
            print >> sys.stdout, "\n"
            self.do_listener_start()

        print >> sys.stdout, "Starting core...\t",
        sys.stdout.flush()

        stdout, stderr = self.syscall("sudo", self.get_scenario_template().format(scenario="startup;"), 
                                      None, "-u", "oracle", "/bin/bash")

        if stdout and stdout.find("Database opened") > -1 \
                and stdout.find("Database mounted") > -1:
            print >> sys.stdout, "done"
        else:
            print >> sys.stdout, "Failed"
            print >> sys.stderr, "Output dump:"
            print >> sys.stderr, stdout

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_db_stop(self):
        """
        Stop SUSE Manager database.
        """
        print >> sys.stdout, "Stopping the SUSE Manager database...\t",
        sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        if ready:
            print >> sys.stdout, "\n"
            self.do_listener_stop()
        else:
            print >> sys.stdout, "Failed"
            print >> sys.stderr, "Error: SUSE Manager database or listener is not running."
            return

        print >> sys.stdout, "Shutting down core...\t",
        sys.stdout.flush()
        stdout, stderr = self.syscall("sudo", self.get_scenario_template().format(scenario="shutdown immediate;"), 
                                None, "-u", "oracle", "/bin/bash")

        if stdout and stdout.find("Database closed") > -1 \
                and stdout.find("Database dismounted") > -1 \
                and stdout.find("instance shut down") > -1:
            print >> sys.stdout, "done"
        else:
            print >> sys.stdout, "Failed"
            print >> sys.stderr, "Output dump:"
            print >> sys.stderr, stdout

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_db_status(self):
        """
        Get SUSE Database running status.
        """
        print >> sys.stdout, "Checking database core...\t",
        sys.stdout.flush()

        running, stdout, stderr = self.get_db_status()
        if running:
            print >> sys.stdout, "online"
        else:
            print >> sys.stdout, "offline"
            


    def do_table_sizes(self):
        """
        Show space report for each table.
        """
        longest = 5
        table = {}
        index = []
        stdout, stderr = self.call_scenario('tablesizes.scn', user=self.config.get('db_user', '').upper())
        for tname, tsize in filter(None, [filter(None, line.replace("\t", " ").split(" ")) for line in stdout.split("\n")]):
            table[tname] = tsize
            index.append(tname)
            longest = len(tname) > longest and len(tname) or longest

        index.sort()

        if table:
            total = 0
            print >> sys.stdout, "%s\t%s" % (("Table" + ((longest - 5) * " ")), "Size")

            for tname in index:
                tsize = int(table[tname])
                print >> sys.stdout, "%s\t%.2fK" % ((tname + ((longest - len(tname)) * " ")), round(tsize / 1024.))
                total += tsize

            print >> sys.stdout, "\n%s\t%.2fM\n" % (("Total" + ((longest - 5) * " ")), round(total / 1024. / 1024.))
            

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr
            raise Exception("Unhandled underlying error.")


    def _do_verify(self):
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


    def get_db_status(self):
        """
        Get Oracle database status.
        """
        scenario = "select 999 as MAGICPING from dual;" # :-)
        stdout, stderr = self.syscall("sudo", self.get_scenario_template().format(scenario=scenario), 
                                None, "-u", "oracle", "/bin/bash")
        return (stdout.find('MAGICPING') > -1 and stdout.find('999') > -1), stdout, stderr



def getGate(config):
    """
    Get gate to the database engine.
    """
    return OracleGate(config)
