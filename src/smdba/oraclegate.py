# Oracle administration gate.
#
# Author: Bo Maryniuk <bo@suse.de>
#
#
# The MIT License (MIT)
# Copyright (C) 2012 SUSE Linux Products GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions: 
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software. 
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE. 
# 

from .basegate import BaseGate
from .basegate import GateException
from .roller import Roller
from .utils import TablePrint

import os
import sys
import re
import time
from . import utils
import random


class HandleInfo:
    """
    Backup handle info
    """
    def __init__(self, availability, handle, recid, stamp):
        self.availability = availability
        self.handle = handle
        self.recid = recid
        self.stamp = stamp


class BackupInfo:
    """
    Backup info object.
    """
    def __init__(self, key, completion, tag):
        self.key = key
        self.completion = completion
        self.tag = tag


class DBStatus:
    """
    Database status result class.
    """

    def __init__(self):
        """
        Init.
        """
        self.ready = False
        self.stderr = None
        self.stdout = None
        self.uptime = ""
        self.unknown = 0
        self.available = 0



class OracleGate(BaseGate):
    """
    Gate for Oracle database tools.
    """
    NAME = "oracle"
    ORATAB = "/etc/oratab"
    LSNR_CTL = "%s/bin/lsnrctl"
    HELPER_CONF = "%s/smdba-helper.conf"


    def __init__(self, config):
        """
        Constructor.
        """
        self.config = config

        # Get Oracle home
        if not os.path.exists(self.ORATAB):
            raise Exception("Underlying error: file \"%s\" does not exists or cannot be read." % self.ORATAB)

        dbsid = self.config.get("db_name")
        for tabline in [_f for _f in [line.strip() for line in open(self.ORATAB).readlines()] if _f]:
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
        os.environ['ORACLE_SID'] = dbsid
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
        self.vw_check_database_ready("Database must be running and ready!", output_shift=2)

        roller = Roller()
        roller.start()
        print("Getting available backups:\t", end=' ', file=sys.stdout)

        class InfoNode:pass
        infoset = []
        stdout, stderr = self.call_scenario('rman-list-backups', target='rman')
        self.to_stderr(stderr)

        roller.stop("finished")
        time.sleep(1)

        if stdout:
            for chunk in [_f for _f in [re.sub('=+', '', c).strip() for c in stdout.split("\n=")[-1].split('BS Key')] if _f]:
                try:
                    info = InfoNode()
                    info.files = []
                    piece_chnk, files_chnk = chunk.split('List of Datafiles')
                    # Get backup place
                    for line in [l.strip() for l in piece_chnk.split("\n")]:
                        if line.lower().startswith('piece name'):
                            info.backup = line.split(" ")[-1]
                        if line.lower().find('status') > -1:
                            status_line = [_f for _f in line.replace(':', '').split("Status")[-1].split(" ") if _f]
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
                            line = [_f for _f in line.split(" ") if _f]
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
                    print("No backup snapshots available.", file=sys.stderr)
                    sys.exit(1)

            # Display backup data
            if (infoset):
                print("Backups available:\n", file=sys.stdout)
                for info in infoset:
                    print("Name:\t", info.backup, file=sys.stdout)
                    print("Files:", file=sys.stdout)
                    for dbf in info.files:
                        print("\tType:", dbf.type, end=' ', file=sys.stdout)
                        print("\tDate:", dbf.date, end=' ', file=sys.stdout)
                        print("\tFile:", dbf.file, file=sys.stdout)
                    print(file=sys.stdout)

            
    def do_backup_purge(self, *args, **params):
        """
        Purge all backups. Useful after successfull reliable recover from the disaster.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to purge assigned backups of it!");

        print("Checking backups:\t", end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()

        info = self.get_backup_info()
        if not len(info):
            roller.stop("failed")
            time.sleep(1)
            print("No backup snapshots available.", file=sys.stderr)
            sys.exit(1)
        roller.stop("finished")
        time.sleep(1)

        print("Removing %s backup%s:\t" % (len(info), len(info) > 1 and 's' or ''), end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()
        stdout, stderr = self.call_scenario('rman-backup-purge', target='rman')
        if stderr:
            roller.stop("failed")
            time.sleep(1)
            self.to_stderr(stderr)
        
        roller.stop("finished")
        time.sleep(1)


    def _backup_rotate(self):
        """
        Rotate backup by purging the obsolete/expired backup set.
        This method is internal and needs to be performed on a healthy, ensured database.
        """
        print("Rotating the backup:\t", end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('rman-hot-backup-roll', target='rman')

        if stderr:
            roller.stop("failed")
            time.sleep(1)
            self.to_stderr(stderr)

        if stdout:
            roller.stop("finished")
            time.sleep(1)


    def do_backup_hot(self, *args, **params):
        """
        Perform hot backup on running database.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to take a backup of it!");

        # Check DBID is around all the time (when DB is healthy!)
        self.get_dbid(known_db_status=True)

        if not self.get_archivelog_mode():
            raise GateException("Archivelog is not turned on.\n\tPlease shutdown SUSE Manager and run system-check first!")

        print("Backing up the database:\t", end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('rman-hot-backup', target='rman')

        if stderr:
            roller.stop("failed")
            time.sleep(1)
            self.to_stderr(stderr)

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

            print("Data files archived:", file=sys.stdout)
            for f in files:
                print("\t" + f, file=sys.stdout)
            print(file=sys.stdout)

            print("Archive logs:", file=sys.stdout)
            for arc in arclogs:
                print("\t" + arc, file=sys.stdout)
            print(file=sys.stdout)

        # Rotate and check
        self.autoresolve_backup()
        self._backup_rotate()

        # Finalize
        hb, fb, ha, fa = self.check_backup_info()
        print("Backup summary as follows:", file=sys.stdout)
        if len(hb):
            print("\tBackups:", file=sys.stdout)
            for bkp in hb:
                print("\t\t", bkp.handle, file=sys.stdout)
            print(file=sys.stdout)

        if len(ha):
            print("\tArchive logs:", file=sys.stdout)
            for bkp in ha:
                print("\t\t", bkp.handle, file=sys.stdout)
            print(file=sys.stdout)
        
        if len(fb):
            print("WARNING! Broken backups has been detected:", file=sys.stderr)
            for bkp in fb:
                print("\t\t", bkp.handle, file=sys.stderr)
            print(file=sys.stderr)

        if len(fa):
            print("WARNING! Broken archive logs has been detected:", file=sys.stderr)
            for bkp in fa:
                print("\t\t", bkp.handle, file=sys.stderr)
            print(file=sys.stderr)
        print("\nFinished.", file=sys.stdout)

        
    def do_backup_check(self, *args, **params):
        """
        Check the consistency of the backup.
        @help
        autoresolve\t\tTry to automatically resolve errors and inconsistencies.\n
        """
        self.vw_check_database_ready("Database must be healthy and running in order to check assigned backups of it!");

        info = self.get_backup_info()
        if len(info):
            print("Last known backup:", info[0].completion, file=sys.stdout)
        else:
            raise GateException("No backups has been found!")
        
        hb, fb, ha, fa = self.check_backup_info()
        # Display backups info
        if fb:
            print("WARNING! Failed backups has been found as follows:", file=sys.stderr)
            for bkp in fb:
                print("\tName:", bkp.handle, file=sys.stderr)
            print(file=sys.stderr)
        else:
            print(("%s available backup%s seems healthy." % (len(hb), len(hb) > 1 and 's are' or '' )), file=sys.stdout)
        
        # Display ARCHIVELOG info
        if fa:
            print("WARNING! Failed archive logs has been found as follows:", file=sys.stderr)
            for arc in fa:
                print("\tName:", arc.handle, file=sys.stderr)
            print(file=sys.stderr)
            if 'autoresolve' not in args:
                print("Try using \"autoresolve\" directive.", file=sys.stderr)
                sys.exit(1)
            else:
                self.autoresolve_backup()
                hb, fb, ha, fa = self.check_backup_info()
                if fa:
                    print("WARNING! Still are failed archive logs:", file=sys.stderr)
                    for arc in fa:
                        print("\tName:", arc.handle, file=sys.stderr)
                        print(file=sys.stderr)
                    if 'ignore-errors' not in args:
                        print("Maybe you want to try \"ignore-errors\" directive and... cross the fingers.", file=sys.stderr)
                        sys.exit(1)
                else:
                    print("Hooray! No failures in backups found!", file=sys.stdout)
        else:
            print(("%s available archive log%s seems healthy." % (len(ha), len(ha) > 1 and 's are' or '' )), file=sys.stdout)


    def do_backup_restore(self, *args, **params):
        """
        Restore the SUSE Manager database from backup.
        @help
        force\t\t\tShutdown database prior backup, if running.
        start\t\t\tAttempt to start a database after restore.
        --strategy=<value>\tManually force strategry 'full' or 'partial'. Don't do that.
        """
        dbid = self.get_dbid()
        scenario = {
            'full':'rman-recover-ctl',
            'partial':'rman-recover',
        }

        # Control file still around?
        strategy = None
        if params.get("strategy") in ['full', 'partial']:
            strategy = params.get("strategy")
        elif params.get("strategy") is not None:
            raise GateException("Unknown value %s for option 'strategy'. Please read 'help' first." % params.get("strategy"))
        
        if not strategy:
            strategy = "full"
            db_path = os.environ['ORACLE_BASE'] + "/oradata/" + os.environ['ORACLE_SID']
            for fname in os.listdir(db_path):
                if fname.lower().endswith(".ctl"):
                    strategy = "partial"
                    break

        print(("Restoring the SUSE Manager Database using %s strategy" % strategy), file=sys.stdout)

        # Could be database just not cleanly killed
        # In this case great almighty RMAN won't connect at all and just crashes. :-(
        self.do_db_start()
        self.do_db_stop()

        print("Preparing database:\t", end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()

        dbstatus = self.get_db_status()
        if dbstatus.ready:
            if 'force' in args:
                roller.stop("running")
                time.sleep(1)
                self.do_db_stop()
            else:
                roller.stop("failed")
                time.sleep(1)
                raise GateException("Database must be put offline. Or use options (run \"help\" for this procedure).")
        else:
            roller.stop("success")
            time.sleep(1)
        
        print("Restoring from backup:\t", end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario(scenario[strategy], target='rman', dbid=str(dbid))
        
        if stderr:
            roller.stop("failed")
            time.sleep(1)
            self.to_stderr(stderr)
            
        if stdout:
            roller.stop("finished")
            time.sleep(1)

        self.do_db_stop()

        if 'start' in args:
            self.do_db_start()
            self.do_listener_status()


    def do_stats_refresh(self, *args, **params):
        """
        Gather statistics on SUSE Manager database objects.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to get statistics of it!");

        print("Gathering statistics on SUSE Manager database...\t", end=' ', file=sys.stdout)

        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('gather-stats', owner=self.config.get('db_user', '').upper())

        if stdout and stdout.strip() == 'done':
            roller.stop('finished')
        else:
            roller.stop('failed')

        time.sleep(1)
        self.to_stderr(stderr)
            

    def do_space_overview(self, *args, **params):
        """
        Show database space report.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to get space overview!");
        stdout, stderr = self.call_scenario('report')
        self.to_stderr(stderr)
        
        ora_error = self.has_ora_error(stdout)
        if ora_error:
            raise GateException("Please visit http://%s.ora-code.com/ page to know more details." % ora_error.lower())

        table = [("Tablespace", "Avail (Mb)", "Used (Mb)", "Size (Mb)", "Use %",),]
        for name, free, used, size in [" ".join([_f for _f in line.replace("\t", " ").split(" ") if _f]).split(" ") 
                                       for line in stdout.strip().split("\n")[2:]]:
            table.append((name, free, used, size, str(int(float(used) / float(size) * 100)),))
        print("\n", TablePrint(table), "\n", file=sys.stdout)


    def do_stats_overview(self, *args, **params):
        """
        Show tables with stale or empty statistics.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to get stats overview!");
        print("Preparing data:\t\t", end=' ', file=sys.stdout)
        
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('stats', owner=self.config.get('db_user', '').upper())

        roller.stop('finished')
        time.sleep(1)

        self.to_stderr(stderr)

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
                    print("Ignoring", repr(line))

        if stale:
            print("\nList of stale objects:", file=sys.stdout)
            for obj in stale:
                print("\t", obj, file=sys.stdout)
            print("\nFound %s stale objects\n" % len(stale), file=sys.stdout)
        else:
            print("No stale objects found", file=sys.stdout)

        if empty:
            print("\nList of empty objects:", file=sys.stdout)
            for obj in empty:
                print("\t", obj, file=sys.stdout)
            print("\nFound %s objects that currently have no statistics.\n" % len(stale), file=sys.stdout)
        else:
            print("No empty objects found.", file=sys.stdout)

        if stderr:
            print("Error dump:", file=sys.stderr)
            print(stderr, file=sys.stderr)


    def do_space_reclaim(self, *args, **params):
        """
        Free disk space from unused object in tables and indexes.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to reclaim the used space!");        

        print("Examining the database...\t", end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('shrink-segments-advisor')
        stderr = None

        if stderr:
            roller.stop('failed')
            time.sleep(1)
            self.to_stderr(stderr)
        else:
            roller.stop('done')
            time.sleep(1)

        print("Gathering recommendations...\t", end=' ', file=sys.stdout)

        roller = Roller()
        roller.start()

        # get the recomendations
        stdout, stderr = self.call_scenario('recomendations')

        if not stdout and not stderr:
            roller.stop("finished")
            time.sleep(1)
            print("\nNo space reclamation possible at this time.\n", file=sys.stdout)
            return

        elif stdout:
            roller.stop("done")
            time.sleep(1)

        else:
            roller.stop("failed")
            time.sleep(1)
            print("Error dump:", file=sys.stderr)
            print(stderr, file=sys.stderr)


        messages = {
            'TABLE' : 'Tables',
            'INDEX' : 'Indexes',
            'AUTO' : 'Recommended segments',
            'MANUAL' : 'Non-shrinkable tablespace',
            }

        tree = {}
        wseg = 0

        if stdout:
            lines = [tuple([_f for _f in line.strip().replace("\t", " ").split(" ") if _f]) for line in stdout.strip().split("\n")]
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
                print("\nTablespace:", tsn, file=sys.stdout)
                for obj in tree[tsn].keys():
                    print("\n\t" +  messages.get(obj, "Object: " + obj), file=sys.stdout)
                    for stype in tree[tsn][obj].keys():
                        typetotal = 0
                        print("\t" + messages.get(stype, "Type: " + stype), file=sys.stdout)
                        for segment, size in tree[tsn][obj][stype]:
                            print("\t\t", \
                                (segment + ((wseg - len(segment)) * " ")), \
                                "\t", '%.2fM' % (size / 1024. / 1024.), file=sys.stdout)
                            total += size
                            typetotal += size
                        total_message = "Total " + messages.get(obj, '').lower()
                        print("\n\t\t", \
                            (total_message + ((wseg - len(total_message)) * " ")), \
                            "\t", '%.2fM' % (typetotal / 1024. / 1024.), file=sys.stdout)

            print("\nTotal reclaimed space: %.2fGB" % (total / 1024. / 1024. / 1024.), file=sys.stdout)

        # Reclaim space
        if tree:
            for tsn in tree.keys():
                for obj in tree[tsn].keys():
                    if tree[tsn][obj].get('AUTO', None):
                        print("\nReclaiming space on %s:" % messages[obj].lower(), file=sys.stdout)
                        for segment, size in tree[tsn][obj]['AUTO']:
                            print("\t", segment + "...\t", end=' ', file=sys.stdout)
                            sys.stdout.flush()
                            stdout, stderr = self.syscall("sudo", self.get_scenario_template().replace('@scenario', self.__get_reclaim_space_statement(segment, obj)),
                                                          None, "-u", "oracle", "/bin/bash")
                            if stderr:
                                print("failed", file=sys.stdout)
                                print(stderr, file=sys.stderr)
                            else:
                                print("done", file=sys.stdout)

        print("Reclaiming space finished", file=sys.stdout)


    def __get_reclaim_space_statement(self, segment, obj):
            query = []
            if obj != 'INDEX':
                query.append("alter %s %s.%s enable row movement;" % (obj, self.config.get('db_user', '').upper(), segment))
            query.append("alter %s %s.%s shrink space compact;" % (obj, self.config.get('db_user', '').upper(), segment))
            query.append("alter %s %s.%s deallocate unused space;" % (obj, self.config.get('db_user', '').upper(), segment))
            query.append("alter %s %s.%s coalesce;" % (obj, self.config.get('db_user', '').upper(), segment))

            return '\n'.join(query)


    def do_listener_start(self, *args, **params):
        """
        Start the SUSE Manager database listener.
        """
        if not 'quiet' in args:
            print("Starting database listener...\t", end=' ', file=sys.stdout)
            sys.stdout.flush()

        dbstatus = self.get_status()
        if dbstatus.ready:
            if not 'quiet' in args:
                print("Failed", file=sys.stdout)
                print("Error: listener already running.", file=sys.stderr)
            return

        ready = False
        stdout, stderr = self.syscall("sudo", None, None, "-u", "oracle", "ORACLE_HOME=" + self.ora_home, self.lsnrctl, "start")
        if stdout:
            for line in stdout.split("\n"):
                if line.lower().startswith("uptime"):
                    ready = True
                    break

            if not 'quiet' in args:
                print((ready and "done" or "failed"), file=sys.stdout)

        if stderr and not 'quiet' in args:
            self.to_stderr(stderr)


    def do_listener_stop(self, *args, **params):
        """
        Stop the SUSE Manager database listener.
        @help
        quiet\tSuppress any output.
        """
        if not 'quiet' in args:
            print("Stopping database listener...\t", end=' ', file=sys.stdout)
            sys.stdout.flush()

        dbstatus = self.get_status()
        if not dbstatus.ready:
            if not 'quiet' in args:
                print("Failed", file=sys.stdout)
                print("Error: listener is not running.", file=sys.stderr)
                return

        success = False
        stdout, stderr = self.syscall("sudo", None, None, "-u", "oracle", "ORACLE_HOME=" + self.ora_home, self.lsnrctl, "stop")
        
        if stdout:
            for line in stdout.split("\n"):
                if line.lower().find("completed successfully") > -1:
                    success = True
                    break

            if not 'quiet' in args:
                print((success and "done" or "failed"), file=sys.stdout)

        if stderr and not 'quiet' in args:
            self.to_stderr(stderr)


    def do_listener_status(self, *args, **params):
        """
        Show database status.
        """
        print("Listener:\t", end=' ', file=sys.stdout)
        sys.stdout.flush()

        dbstatus = self.get_status()
        print((dbstatus.ready and "running" or "down"), file=sys.stdout)
        print("Uptime:\t\t", dbstatus.uptime and dbstatus.uptime or "", file=sys.stdout)
        print("Instances:\t", dbstatus.available, file=sys.stdout)
        
        if dbstatus.stderr:
            print("Error dump:", file=sys.stderr)
            print(dbstatus.stderr, file=sys.stderr)

        if dbstatus.unknown:
            print("Warning: %s unknown instance%s." % (dbstatus.unknown, dbstatus.unknown > 1 and 's' or ''), file=sys.stderr)
        if not dbstatus.available:
            print("Critical: No available instances found!", file=sys.stderr)


    def do_listener_restart(self, *args, **params):
        """
        Restart SUSE Manager database listener.
        """
        print("Restarting listener...", end=' ', file=sys.stdout)
        sys.stdout.flush()

        dbstatus = self.get_status()
        if dbstatus.ready:
            self.do_listener_stop()

        self.do_listener_start()

        print("done", file=sys.stdout)


    def do_db_start(self, *args, **params):
        """
        Start SUSE Manager database.
        """
        print("Starting listener:\t", end=' ', file=sys.stdout)
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        dbstatus = self.get_status()
        if dbstatus.ready:
            roller.stop('failed')
            time.sleep(1)
            raise GateException("Error: listener is already running")
        else:
            self.do_listener_start('quiet')

        roller.stop('done')
        time.sleep(1)

        print("Starting core...\t", end=' ', file=sys.stdout)
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        stdout, stderr = self.syscall("sudo", None, None, "-u", "oracle", self.ora_home + "/bin/dbstart")
        roller.stop('done')
        time.sleep(1)

        self.to_stderr(stderr)
    
        if stdout and stdout.find("Database opened") > -1 \
                and stdout.find("Database mounted") > -1:
            roller.stop('done')
            time.sleep(1)
        else:
            roller.stop('failed')
            time.sleep(1)
            print("Output dump:", file=sys.stderr)
            print(stdout, file=sys.stderr)

        if stderr:
            print("Error dump:", file=sys.stderr)
            print(stderr, file=sys.stderr)


    def do_db_stop(self, *args, **params):
        """
        Stop SUSE Manager database.
        """
        print("Stopping the SUSE Manager database...", file=sys.stdout)
        sys.stdout.flush()

        print("Stopping listener:\t", end=' ', file=sys.stdout)
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        dbstatus = self.get_status()
        if dbstatus.ready:
            self.do_listener_stop(*['quiet'])
            roller.stop("done")
            time.sleep(1)
        else:
            roller.stop("not running")
            time.sleep(1)

        print("Stopping core:\t\t", end=' ', file=sys.stdout)
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        dbstatus = self.get_db_status()
        if not dbstatus.ready:
            roller.stop("failed")
            time.sleep(1)
            raise GateException("Error: database core is already offline.")

        stdout, stderr = self.syscall("sudo", None, None, "-u", "oracle", self.ora_home + "/bin/dbshut")
        if stderr:
            roller.stop("failed")
            time.sleep(1)
        else:
            roller.stop("done")
            time.sleep(1)

        self.to_stderr(stderr)


    def do_db_status(self, *args, **params):
        """
        Display SUSE Manager database runtime status.
        """
        print("Checking database core...\t", end=' ', file=sys.stdout)
        sys.stdout.flush()

        dbstatus = self.get_db_status()
        if dbstatus.ready:
            print("online", file=sys.stdout)
        else:
            print("offline", file=sys.stdout)
            


    def do_space_tables(self, *args, **params):
        """
        Show space report for each table.
        """
        dbstatus = self.get_db_status()
        if not dbstatus.ready:
            raise GateException("Database is not running!")

        table = [('Table', 'Size',)]
        total = 0
        stdout, stderr = self.call_scenario('tablesizes', user=self.config.get('db_user', '').upper())
        self.to_stderr(stderr)
        ora_error = self.has_ora_error(stdout)
        if ora_error:
            raise GateException("Please visit http://%s.ora-code.com/ page to know more details." % ora_error.lower())

        for tname, tsize in [_f for _f in [[_f for _f in line.replace("\t", " ").split(" ") if _f] for line in stdout.split("\n")] if _f]:
            table.append((tname, ('%.2fK' % round(float(tsize) / 1024.)),))
            total += float(tsize)
        table.append(('', '',))
        table.append(('Total', ('%.2fM' % round(total / 1024. / 1024.))))

        if table:
            print("\n", TablePrint(table), "\n", file=sys.stdout)

        if stderr:
            print("Error dump:", file=sys.stderr)
            print(stderr, file=sys.stderr)
            raise Exception("Unhandled underlying error.")


    def do_db_check(self, *args, **params):
        """
        Check full connection to the database.
        """
        print("Checking connection:\t", end=' ', file=sys.stdout)
        sys.stdout.flush()
        roller = Roller()
        roller.start()
        login = '%s/%s@%s' % (self.config.get('db_user'),
                              self.config.get('db_password'),
                              self.config.get('db_name'))
        roller.stop(self.get_db_status(login=login).ready and "ready" or "not available")
        time.sleep(1)


    #
    # Helpers below
    #

    def get_status(self):
        """
        Get Oracle listener status.
        """
        status = DBStatus()
        status.stdout, status.stderr = self.syscall("sudo", None, None, "-u", "oracle", "ORACLE_HOME=" + self.ora_home, self.lsnrctl, "status")
    
        if status.stdout:
            for line in status.stdout.split("\n"):
                if line.lower().startswith("uptime"):
                    status.uptime = line.replace("\t", " ").split(" ", 1)[-1].strip()
                    status.ready = True
                    break
            status.unknown = 0
            status.available = 0
            for line in status.stdout.split('Services')[-1].split("\n"):
                if line.find('READY') > -1:
                    status.available += 1
                if line.find('UNKNOWN') > -1:
                    status.unknown += 1

        return status


    def get_db_status(self, login=None):
        """
        Get Oracle database status.
        """
        status = DBStatus()
        mnum = 'm' + str(random.randint(0xff, 0xfff))
        scenario = "select '%s' as MAGICPING from dual;" % mnum # :-)
        status.stdout, status.stderr = self.syscall("sudo", self.get_scenario_template(login=login).replace('@scenario', scenario), 
                                                    None, "-u", "oracle", "/bin/bash")
        status.ready = False
        for line in [line.strip() for line in status.stdout.lower().split("\n")]:
            if line == mnum:
                status.ready = True
                break

        return status


    def check(self):
        """
        Check system requirements for this gate.
        """
        if not os.path.exists(self.ora_home + "/bin/sqlplus"):
            raise GateException("Cannot find operation sub-component, required for the gate.")
        elif not os.path.exists(self.ora_home + "/bin/rman"):
            raise GateException("Cannot find backup sub-component, required for the gate.")

        return True


    def do_system_check(self, *args, **params):
        """
        Common backend healthcheck.
        @help
        force-archivelog-off\tForce archivelog mode to off.
        """
        print("Checking SUSE Manager database backend\n", file=sys.stdout)

        # Set data table autoextensible.
        stdout, stderr = self.call_scenario('cnf-get-noautoext')
        if stderr:
            print("Autoextend check error:", file=sys.stderr)
            print(stderr, file=sys.stderr)
            raise GateException("Unable continue system check")

        if stdout:
            print("Autoextensible:\tOff", file=sys.stdout)
            scenario = []
            [scenario.append("alter database datafile '%s' autoextend on;" % fname) for fname in stdout.strip().split("\n")]
            self.syscall("sudo", self.get_scenario_template().replace('@scenario', '\n'.join(scenario)), 
                         None, "-u", "oracle", "/bin/bash")
            print("%s table%s has been autoextended" % (len(scenario), len(scenario) > 1 and 's' or ''), file=sys.stdout)
        else:
            print("Autoextensible:\tYes", file=sys.stdout)

        # Turn on archivelog.
        #
        if 'force-archivelog-off' in args:
            if self.get_archivelog_mode():
                self.set_archivelog_mode(status=False)
            else:
                print("Archivelog mode is not used.", file=sys.stdout)
        else:
            if not self.get_archivelog_mode():
                self.set_archivelog_mode(True)
                if not self.get_archivelog_mode():
                    print("No archive log", file=sys.stderr)
                else:
                    print("Database is now running in archivelog mode.", file=sys.stdout)
            else:
                print("Archivelog:\tYes", file=sys.stdout)

        # Free space on the storage.
        #
        # TBD

        print("\nFinished\n", file=sys.stdout)


    def set_archivelog_mode(self, status=True):
        """
        Set archive log mode status.
        """
        print(("Turning %s archivelog mode...\t" % (status and 'on' or 'off')), end=' ', file=sys.stdout)
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        stdout, stderr = None, None
        success, failed = "done", "failed"
        if status:
            destination = os.environ['ORACLE_BASE'] + "/oradata/" + os.environ['ORACLE_SID'] + "/archive"
            stdout, stderr = self.call_scenario('ora-archivelog-on', destination=destination)
        else:
            stdout, stderr = self.call_scenario('ora-archivelog-off')
            success, failed = "failed", "done"

        if self.get_archivelog_mode():
            roller.stop(success)
        else:
            roller.stop(failed)

        time.sleep(1)


    def get_archivelog_mode(self):
        """
        Get archive log mode status.
        """
        stdout, stderr = self.call_scenario('ora-archivelog-status')
        if stdout:
            for line in stdout.split("\n"):
                line = line.strip()
                if line == 'NOARCHIVELOG':
                    return False

        return True


    def get_dbid(self, path=None, known_db_status=False):
        """
        Get DBID and save it.
        """
        # Get DBID from the database and save it.
        # If database is broken, get last saved.
        if not path:
            path = os.environ['ORACLE_BASE'] + "/smdba"

        if not os.path.exists(path):
            os.makedirs(path)
        
        # Add full filename
        path = self.HELPER_CONF % path

        stdout, stderr = self.call_scenario('ora-dbid')
        dbid = None
        if stdout:
            try:
                dbid = int(stdout.split("\n")[-1])
            except:
                # Failed to get dbid anyway, let's just stay silent for now.
                if known_db_status:
                    raise GateException("The data in the database is not reachable!")
        if dbid:
            fg = open(path, 'w')
            fg.write("# Database ID of \"%s\", please don't lose it ever.\n") 
            fg.write(os.environ['ORACLE_SID'] + ".dbid=%s\n" % dbid)
            fg.close()
        elif os.path.exists(path):
            for line in open(path).readlines():
                line = line.strip()
                if not line or line.startswith('#') or (line.find('=') == -1):
                    continue
                dbidkey, dbid = [el.strip() for el in line.split('=', 1)]
                if dbid:
                    try:
                        dbid = int(dbid)
                    except:
                        # Failed get dbid again.
                        pass

        if not dbid:
            raise GateException("Looks like your backups was never taken with the SMDBA.\n\tGood luck with the tools you used before!")

        return dbid
    

    def has_ora_error(self, raw):
        """
        Just look if output was not crashed. Because Oracle developers often
        cannot decide to where to send an error: to STDERR or STDOUT. :-)
        """
        if raw is None:
            raw = ''
            
        raw = raw.strip()
        if not raw:
            return None

        for line in raw.split('\n'):
            ftkn = [_f for _f in line.split(" ") if _f][0]
            if ftkn.startswith('ORA-') and ftkn.endswith(':'):
                err = None
                try:
                    err = int(ftkn[4:-1])
                except:
                    # No need to report this at all.
                    pass
                if err:
                    return ftkn[:-1]


    def autoresolve_backup(self):
        """
        Try to autoresolve backup inconsistencies.
        """
        self.call_scenario('rman-backup-autoresolve', target='rman')

                
    def check_backup_info(self):
        """
        Check if backup is consistent.
        """
        failed_backups = []
        healthy_backups = []
        failed_archivelogs = []
        healthy_archivelogs = []
        bkpsout = None
        arlgout = None

        # Get database backups
        stdout, stderr = self.call_scenario('rman-backup-check-db', target='rman')
        if stderr:
            print("Backup information check failure:", file=sys.stderr)
            print(stderr, file=sys.stderr)
            raise GateException("Unable to check the backups.")

        for chunk in stdout.split("RMAN>"):
            chunk = chunk.strip()
            if not chunk:
                continue
            if chunk.find("crosschecked backup piece") > -1:
                bkpsout = chunk
                break

        # Get database archive logs check
        stdout, stderr = self.call_scenario('rman-backup-check-al', target='rman')
        if stderr:
            print("Archive log information check failure:", file=sys.stderr)
            print(stderr, file=sys.stderr)
            raise GateException("Unable to check the archive logs backup.")
        
        for chunk in stdout.split("RMAN>"):
            chunk = chunk.strip()
            if not chunk:
                continue
            if chunk.find("archived log file name") > -1:
                arlgout = chunk
                break

        # Check failed backups
        if bkpsout:
            for line in [elm.strip() for elm in bkpsout.split("crosschecked")]:
                if not line.startswith("backup piece"):
                    continue
                obj_raw = line.split("\n")[:2]
                if len(obj_raw) == 2:
                    status = obj_raw[0].strip().split(" ")[-1].replace("'", '').lower()
                    data = dict([_f for _f in ["=" in elm and tuple(elm.split("=", 1)) or None for elm in [_f for _f in obj_raw[-1].split(" ") if _f]] if _f])
                    hinfo = HandleInfo(status, handle=data['handle'], recid=data['RECID'], stamp=data['STAMP'])
                    if hinfo.availability == 'available':
                        healthy_backups.append(hinfo)
                    else:
                        failed_backups(hinfo)

        # Check failed archive logs
        if arlgout:
            for archline in [elm.strip() for elm in arlgout.split("validation", 1)[-1].split("Crosschecked")[0].split("validation")]:
                obj_raw = archline.split("\n")
                if len(obj_raw) == 2:
                    status = obj_raw[0].split(" ")[0]
                    data = dict([_f for _f in ['=' in elm and tuple(elm.split('=', 1)) or None for elm in obj_raw[1].split(" ")] if _f])
                    hinfo = HandleInfo(status == 'succeeded' and 'available' or 'unavailable', recid=data['RECID'], stamp=data['STAMP'], handle=data['name']) # Ask RMAN devs why this time it is called "name"
                    if hinfo.availability == 'available':
                        healthy_archivelogs.append(hinfo)
                    else:
                        failed_archivelogs.append(hinfo)

        return healthy_backups, failed_backups, healthy_archivelogs, failed_archivelogs


    def get_backup_info(self):
        """
        Return list of BackupInfo objects, representing backups.
        """
        stdout, stderr = self.call_scenario('rman-backup-info', target='rman')
        if stderr:
            print("Backup information listing failure:", file=sys.stderr)
            print(stderr, file=sys.stderr)
            raise GateException("Unable to get information about backups.")

        capture = False
        idx = []
        info = {}
        for line in stdout.split("\n"):
            line = line.strip()
            if line.startswith("---"): # Table delimeter
                capture = True
                continue

            if capture:
                if not line:
                    capture = False
                    continue
                tkn = [_f for _f in line.replace("\t", " ").split(" ") if _f]
                info[tkn[5]] = BackupInfo(tkn[0], tkn[5], tkn[-1])
                idx.append(tkn[5])

        return [info[bid] for bid in reversed(sorted(idx))]


    def vw_check_database_ready(self, message, output_shift=1):
        """
        Check if database is ready. Otherwise crash with the given message.
        """
        print("Checking the database:" + ("\t" * output_shift), end=' ', file=sys.stdout)
        roller = Roller()
        roller.start()
        dbstatus = self.get_db_status()
        if dbstatus.ready:
            roller.stop("running")
            time.sleep(1)
        else:
            roller.stop("failed")
            time.sleep(1)
            raise GateException(message);


    def get_current_rfds(self):
        """
        Get current recovery file destination size.
        """
        stdout, stderr = self.call_scenario('ora-archive-info')
        stdout = stdout and stdout.lower()
        curr_fds = ""
        if stdout and stdout.find("db_recovery_file_dest_size") > -1:
            for line in stdout.split("\n"):
                if line.find("db_recovery_file_dest_size") > -1:
                    curr_fds = (line.split(" ")[-1] + "").upper()

        return curr_fds.replace("B", "")

    def get_current_fra_dir(self):
        """
        Get current recovery area directory.
        """
        stdout, stderr = self.call_scenario('ora-archive-fra-dir')
        return (stdout or '/opt/apps/oracle/flash_recovery_area').strip()


    def autoresize_available_archive(self, target_fds):
        """
        Set Oracle environment always up to the current media size.
        """
        stdout, stderr = self.call_scenario('ora-archive-setup', destsize=target_fds)
        if stdout.find("System altered") > -1:
            return True

        print("ERROR:", stderr, file=sys.stderr)

        return False



    def startup(self):
        """
        Hooks before the Oracle gate operations starts.
        """
        # Do we have sudo permission?
        self.check_sudo('oracle')

        # Always set FRA to the current size of the media.
        curr_fds = self.get_current_rfds()
        target_fds = self.size_pretty(self.media_usage(self.get_current_fra_dir())['free'], int_only=True, no_whitespace=True).replace("B", "")

        if curr_fds != target_fds:
            print("WARNING: Reserved space for the backup is smaller than available disk space. Adjusting.", file=sys.stderr)
            if not self.autoresize_available_archive(target_fds):
                print("WARNING: Could not adjust system for backup reserved space!", file=sys.stderr)
            else:
                print("INFO: System settings for the backup recovery area has been altered successfully.", file=sys.stdout)


    def finish(self):
        """
        Hooks after the Oracle gate operations finished.
        """
        pass



def getGate(config):
    """
    Get gate to the database engine.
    """
    return OracleGate(config)
