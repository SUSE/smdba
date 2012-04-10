#
# Oracle administration gate.
#
# Author: Bo Maryniuk <bo@suse.de>
#

from basegate import BaseGate
from basegate import GateException
from roller import Roller

import os
import sys
import re
import time



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


    #
    # Exposed operations below
    #

    def do_backup_list(self, *args, **params):
        """
        List of available backups.
        """
        roller = Roller()
        roller.start()
        print >> sys.stdout, "Getting available backups:\t",

        class InfoNode:pass
        infoset = []
        stdout, stderr = self.call_scenario('rman-list-backups.scn', target='rman')

        roller.stop("finished")
        time.sleep(1)

        if stdout:
            for chunk in filter(None, [re.sub('=+', '', c).strip() for c in stdout.split("\n=")[-1].split('BS Key')]):
                try:
                    info = InfoNode()
                    info.files = []
                    piece_chnk, files_chnk = chunk.split('List of Datafiles')
                    # Get backup place
                    for line in [l.strip() for l in piece_chnk.split("\n")]:
                        if line.lower().startswith('piece name'):
                            info.backup = line.split(" ")[-1]
                        if line.lower().find('status') > -1:
                            status_line = filter(None, line.replace(':', '').split("Status")[-1].split(" "))
                            if len(status_line) ==  5:
                                info.status = status_line[0]
                                info.compression = status_line[2]
                                info.tag = status_line[4]

                    # Get the list of files
                    cutoff = True
                    for line in [l.strip() for l in files_chnk.split("\n")]:
                        if line.startswith('-'):
                            cutoff = None
                            continue
                        else:
                            line = filter(None, line.split(" "))
                            if len(line) > 4:
                                if line[0] == 'File':
                                    continue
                                dbf = InfoNode()
                                dbf.type = line[1]
                                dbf.file = line[-1]
                                dbf.date = line[-2]
                                info.files.append(dbf)
                    infoset.append(info)
                except:
                    print "Nope"

            # Display backup data
            if (infoset):
                print >> sys.stdout, "Backups available:\n"
                for info in infoset:
                    print >> sys.stdout, "Name:\t", info.backup
                    print >> sys.stdout, "Files:"
                    for dbf in info.files:
                        print >> sys.stdout, "\tType:", dbf.type,
                        print >> sys.stdout, "\tDate:", dbf.date,
                        print >> sys.stdout, "\tFile:", dbf.file
                    print >> sys.stdout

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_backup_hot(self, *args, **params):
        """
        Perform host database backup.

        @help
        --backup-dir\tDirectory for backup data to be stored.
        """
        if not params.get('backup-dir'):
            raise Exception("\tPlease run this as \"%s backup-hot help\" first." % sys.argv[0])

        if not os.path.exists(params.get('backup-dir')):
            raise Exception("\tIs the \"%s\" path does not exists?" % params.get('backup-dir'))

        print >> sys.stdout, "Backing up the database:\t",
        roller = Roller()
        roller.start()
        stdout, stderr = self.call_scenario('rman-hot-backup.scn', target='rman', backupdir=params.get('backup-dir'))

        if stderr:
            roller.stop("failed")
            time.sleep(1)
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr

        if stdout:
            roller.stop("finished")
            time.sleep(1)

            files = []
            arclogs = []
            for line in stdout.split("\n"):
                line = line.strip()
                if line.startswith("input") and line.find('datafile') > -1:
                    files.append(line.split("name=")[-1])
                elif line.startswith("archived"):
                    arclogs.append(line.split("name=")[-1].split(" ")[0])

            print >> sys.stdout, "Data files archived:"
            for f in files:
                print >> sys.stdout, "\t" + f
            print >> sys.stdout

            print >> sys.stdout, "Archive logs:"
            for arc in arclogs:
                print >> sys.stdout, "\t" + arc
            print >> sys.stdout


    def do_backup_restore(self, *args, **params):
        """
        Restore the SUSE Manager Database from backup.
        @help
        force\tShutdown database, if running.
        start\tStart database after restore.
        """
        print >> sys.stdout, "Preparing database:\t",
        roller = Roller()
        roller.start()
        ready, stdout, stderr = self.get_db_status()
        if ready:
            if 'force' in args:
                roller.stop("running")
                time.sleep(1)
                self.do_db_stop()
            else:
                roller.stop("failed")
                time.sleep(1)
                raise GateException("Database must be put offline.")
        else:
            roller.stop("success")
            time.sleep(1)
        
        print >> sys.stdout, "Restoring from backup:\t",
        roller = Roller()
        roller.start()

        stdout, stderr = 1, None #self.call_scenario('rman-hot-backup.scn', target='rman', backupdir=params.get('backup-dir'))

        time.sleep(3)

        if stderr:
            roller.stop("failed")
            time.sleep(1)
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr

        if stdout:
            roller.stop("finished")
            time.sleep(1)

        if 'start' in args:
            self.do_db_start()
            self.do_listener_status()


    def _do_extend(self, *args, **params):
        """
        Increase the SUSE Manager Database Instance tablespace.
        """

    def do_stats_refresh(self, *args, **params):
        """
        Gather statistics on SUSE Manager Database database objects.
        """
        print >> sys.stdout, "Gathering statistics on SUSE Manager database...\t",

        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('gather-stats.scn', owner=self.config.get('db_user', '').upper())

        if stdout and stdout.strip() == 'done':
            roller.stop('finished')
        else:
            roller.stop('failed')

        time.sleep(1)

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr
            


    def do_space_overview(self, *args, **params):
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


    def do_stats_overview(self, *args, **params):
        """
        Show tables with stale or empty statistics.
        """
        print >> sys.stdout, "Preparing data:\t\t",
        
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('stats.scn', owner=self.config.get('db_user', '').upper())

        roller.stop('finished')
        time.sleep(1)

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


    def do_space_reclaim(self, *args, **params):
        """
        Free disk space from unused object in tables and indexes.
        """
        
        ready, stdout, stderr = self.get_db_status()
        if not ready:
            raise Exception("Database is not running.")

        print >> sys.stdout, "Examining the database...\t",

        roller = Roller()
        roller.start()

        # run task
        stdout, stderr = self.call_scenario('shrink-segments-advisor.scn')
        stderr = None

        if stderr:
            roller.stop('failed')
            time.sleep(1)
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr
            return
        else:
            roller.stop('done')
            time.sleep(1)

        print >> sys.stdout, "Gathering recommendations...\t",

        roller = Roller()
        roller.start()

        # get the recomendations
        stdout, stderr = self.call_scenario('recomendations.scn')

        if not stdout and not stderr:
            roller.stop("finished")
            time.sleep(1)
            print >> sys.stdout, "\nNo space reclamation possible at this time.\n"
            return

        elif stdout:
            roller.stop("done")
            time.sleep(1)

        else:
            roller.stop("failed")
            time.sleep(1)
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


        messages = {
            'TABLE' : 'Tables',
            'INDEX' : 'Indexes',
            'AUTO' : 'Recommended segments',
            'MANUAL' : 'Non-shrinkable tablespace',
            }

        tree = {}
        wseg = 0

        if stdout:
            lines = [tuple(filter(None, line.strip().replace("\t", " ").split(" "))) for line in stdout.strip().split("\n")]
            for ssm, sname, rspace, tsn, stype in lines:
                tsns = tree.get(tsn, {})
                stypes = tsns.get(stype, {})
                ssms = stypes.get(ssm, [])
                ssms.append((sname, int(rspace),))
                wseg = len(sname) > wseg and len(sname) or wseg
                stypes[ssm] = ssms
                tsns[stype] = stypes
                tree[tsn] = tsns

            total = 0
            for tsn in tree.keys():
                print >> sys.stdout, "\nTablespace:", tsn
                for obj in tree[tsn].keys():
                    print >> sys.stdout, "\n\t" +  messages.get(obj, "Object: " + obj)
                    for stype in tree[tsn][obj].keys():
                        typetotal = 0
                        print >> sys.stdout, "\t" + messages.get(stype, "Type: " + stype)
                        for segment, size in tree[tsn][obj][stype]:
                            print >> sys.stdout, "\t\t", \
                                (segment + ((wseg - len(segment)) * " ")), \
                                "\t", '%.2fM' % (size / 1024. / 1024.)
                            total += size
                            typetotal += size
                        total_message = "Total " + messages.get(obj, '').lower()
                        print >> sys.stdout, "\n\t\t", \
                            (total_message + ((wseg - len(total_message)) * " ")), \
                            "\t", '%.2fM' % (typetotal / 1024. / 1024.)

            print >> sys.stdout, "\nTotal reclaimed space: %.2fGB" % (total / 1024. / 1024. / 1024.)

        # Reclaim space
        if tree:
            for tsn in tree.keys():
                for obj in tree[tsn].keys():
                    if tree[tsn][obj].get('AUTO', None):
                        print >> sys.stdout, "\nReclaiming space on %s:" % messages[obj].lower()
                        for segment, size in tree[tsn][obj]['AUTO']:
                            print >> sys.stdout, "\t", segment + "...\t",
                            sys.stdout.flush()
                            stdout, stderr = self.syscall("sudo", self.get_scenario_template().replace('@scenario', self.__get_reclaim_space_statement(segment)),
                                                          None, "-u", "oracle", "/bin/bash")
                            if stderr:
                                print >> sys.stdout, "failed"
                                print >> sys.stderr, stderr
                            else:
                                print >> sys.stdout, "done"

        print >> sys.stdout, "Reclaiming space finished"


    def __get_reclaim_space_statement(self, segment):
            query = []
            query.append("alter table %s.%s enable row movement;" % (self.config.get('db_user', '').upper(), segment))
            query.append("alter table %s.%s shrink space compact;" % (self.config.get('db_user', '').upper(), segment))

            return '\n'.join(query)


    def do_listener_start(self, *args, **params):
        """
        Start the SUSE Manager Database listener.
        """
        if not 'quiet' in args:
            print >> sys.stdout, "Starting database listener...\t",
            sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        if ready:
            if not 'quiet' in args:
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

            if not 'quiet' in args:
                print >> sys.stdout, (ready and "done" or "failed")

        if stderr and not 'quiet' in args:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_listener_stop(self, *args, **params):
        """
        Stop the SUSE Manager Database listener.
        @help
        quiet\tSuppress any output.
        """
        if not 'quiet' in args:
            print >> sys.stdout, "Stopping database listener...\t",
            sys.stdout.flush()

        ready, uptime, stderr = self.get_status()
        if not ready:
            if not 'quiet' in args:
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

            if not 'quiet' in args:
                print >> sys.stdout, (success and "done" or "failed")

        if stderr and not 'quiet' in args:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_listener_status(self, *args, **params):
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


    def do_listener_restart(self, *args, **params):
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


    def do_db_start(self, *args, **params):
        """
        Start SUSE Manager database.
        """
        print >> sys.stdout, "Starting listener:\t",
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        ready, uptime, stderr = self.get_status()
        if ready:
            roller.stop('failed')
            time.sleep(1)
            raise GateException("Error: listener is already running")
        else:
            pass
            self.do_listener_start('quiet')

        roller.stop('done')
        time.sleep(1)

        print >> sys.stdout, "Starting core...\t",
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        stdout, stderr = self.syscall("sudo", self.get_scenario_template().replace('@scenario', 'startup;'), 
                                      None, "-u", "oracle", "/bin/bash")

        if stdout and stdout.find("Database opened") > -1 \
                and stdout.find("Database mounted") > -1:
            roller.stop('done')
            time.sleep(1)
        else:
            roller.stop('failed')
            time.sleep(1)
            print >> sys.stderr, "Output dump:"
            print >> sys.stderr, stdout

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr


    def do_db_stop(self, *args, **params):
        """
        Stop SUSE Manager database.
        """
        print >> sys.stdout, "Stopping the SUSE Manager database..."
        sys.stdout.flush()

        print >> sys.stdout, "Stopping listener:\t",
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        ready, uptime, stderr = self.get_status()
        if ready:
            self.do_listener_stop(*['quiet'])
            roller.stop("done")
            time.sleep(1)
        else:
            roller.stop("not running")
            time.sleep(1)

        print >> sys.stdout, "Stopping core:\t\t",
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        ready, uptime, stderr = self.get_db_status()
        if not ready:
            roller.stop("failed")
            time.sleep(1)
            raise GateException("Error: database core is already offline.")

        stdout, stderr = self.syscall("sudo", self.get_scenario_template().replace('@scenario', 'shutdown immediate;'),
                                None, "-u", "oracle", "/bin/bash")

        if stdout and stdout.find("Database closed") > -1 \
                and stdout.find("Database dismounted") > -1 \
                and stdout.find("instance shut down") > -1:
            roller.stop("done")
            time.sleep(1)
        else:
            roller.stop("failed")
            time.sleep(1)

            print >> sys.stderr, "\nOutput dump:"
            print >> sys.stderr, stdout + "\n"

        if stderr:
            print >> sys.stderr, "\nError dump:"
            print >> sys.stderr, stderr + "\n"


    def do_db_status(self, *args, **params):
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
            


    def do_space_tables(self, *args, **params):
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


    def _do_verify(self, *args, **params):
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
        stdout, stderr = self.syscall("sudo", self.get_scenario_template().replace('@scenario', scenario), 
                                None, "-u", "oracle", "/bin/bash")
        ready = False
        for line in [line.strip() for line in stdout.lower().split("\n")]:
            if line == '999':
                ready = True
                break

        return ready, stdout, stderr



def getGate(config):
    """
    Get gate to the database engine.
    """
    return OracleGate(config)
