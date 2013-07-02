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

from basegate import BaseGate
from basegate import GateException
from roller import Roller
from utils import TablePrint

import os
import sys
import re
import time
import utils
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
        print >> sys.stdout, "Getting available backups:\t",

        class InfoNode:pass
        infoset = []
        stdout, stderr = self.call_scenario('rman-list-backups', target='rman')

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
                    print >> sys.stderr, "No backup snapshots available."
                    sys.exit(1)

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

            
    def do_backup_purge(self, *args, **params):
        """
        Purge all backups. Useful after successfull reliable recover from the disaster.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to purge assigned backups of it!");

        print >> sys.stdout, "Checking backups:\t",
        roller = Roller()
        roller.start()

        info = self.get_backup_info()
        if not len(info):
            roller.stop("failed")
            time.sleep(1)
            print >> sys.stderr, "No backup snapshots available."
            sys.exit(1)
        roller.stop("finished")
        time.sleep(1)

        print >> sys.stdout, "Removing %s backup%s:\t" % (len(info), len(info) > 1 and 's' or ''),
        roller = Roller()
        roller.start()
        stdout, stderr = self.call_scenario('rman-backup-purge', target='rman')
        if stderr:
            roller.stop("failed")
            time.sleep(1)
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr
        
        roller.stop("finished")
        time.sleep(1)


    def do_backup_hot(self, *args, **params):
        """
        Perform hot backup on running database.
        """
        #if not params.get('backup-dir'):
        #    raise Exception("\tPlease run this as \"%s backup-hot help\" first." % sys.argv[0])

        #if not os.path.exists(params.get('backup-dir')):
        #    print >> sys.stdout, "Creating \"%s\" path" % params.get('backup-dir')
        #    utils.create_dirs(params.get('backup-dir'), "oracle")

        #owner = utils.get_path_owner(params.get('backup-dir'))
        #if owner.user != 'oracle':
        #    raise Exception("\tDirectory \"%s\" does not have proper permissions!" % params.get('backup-dir'))

        self.vw_check_database_ready("Database must be healthy and running in order to take a backup of it!");

        # Check DBID is around all the time (when DB is healthy!)
        self.get_dbid(known_db_status=True)

        if not self.get_archivelog_mode():
            raise GateException("Archivelog is not turned on.\n\tPlease shutdown SUSE Manager and run system-check first!")

        print >> sys.stdout, "Backing up the database:\t",
        roller = Roller()
        roller.start()
        #stdout, stderr = self.call_scenario('rman-hot-backup', target='rman', backupdir=params.get('backup-dir'))
        stdout, stderr = self.call_scenario('rman-hot-backup', target='rman')

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

        # Finalize
        self.autoresolve_backup()
        hb, fb, ha, fa = self.check_backup_info()
        print >> sys.stdout, "Backup summary as follows:"
        if len(hb):
            print >> sys.stdout, "\tBackups:"
            for bkp in hb:
                print >> sys.stdout, "\t\t", bkp.handle
            print >> sys.stdout

        if len(ha):
            print >> sys.stdout, "\tArchive logs:"
            for bkp in ha:
                print >> sys.stdout, "\t\t", bkp.handle
            print >> sys.stdout
        
        if len(fb):
            print >> sys.stderr, "WARNING! Broken backups has been detected:"
            for bkp in fb:
                print >> sys.stderr, "\t\t", bkp.handle
            print >> sys.stderr

        if len(fa):
            print >> sys.stderr, "WARNING! Broken archive logs has been detected:"
            for bkp in fa:
                print >> sys.stderr, "\t\t", bkp.handle
            print >> sys.stderr
        print >> sys.stdout, "\nFinished."

        
    def do_backup_check(self, *args, **params):
        """
        Check the consistency of the backup.
        @help
        autoresolve\t\tTry to automatically resolve errors and inconsistencies.\n
        """
        self.vw_check_database_ready("Database must be healthy and running in order to check assigned backups of it!");

        info = self.get_backup_info()
        if len(info):
            print >> sys.stdout, "Last known backup:", info[0].completion
        else:
            raise GateException("No backups has been found!")
        
        hb, fb, ha, fa = self.check_backup_info()
        # Display backups info
        if fb:
            print >> sys.stderr, "WARNING! Failed backups has been found as follows:"
            for bkp in fb:
                print >> sys.stderr, "\tName:", bkp.handle
            print >> sys.stderr
        else:
            print >> sys.stdout, ("%s available backup%s seems healthy." % (len(hb), len(hb) > 1 and 's are' or '' ))
        
        # Display ARCHIVELOG info
        if fa:
            print >> sys.stderr, "WARNING! Failed archive logs has been found as follows:"
            for arc in fa:
                print >> sys.stderr, "\tName:", arc.handle
            print >> sys.stderr
            if 'autoresolve' not in args:
                print >> sys.stderr, "Try using \"autoresolve\" directive."
                sys.exit(1)
            else:
                self.autoresolve_backup()
                hb, fb, ha, fa = self.check_backup_info()
                if fa:
                    print >> sys.stderr, "WARNING! Still are failed archive logs:"
                    for arc in fa:
                        print >> sys.stderr, "\tName:", arc.handle
                        print >> sys.stderr
                    if 'ignore-errors' not in args:
                        print >> sys.stderr, "Maybe you want to try \"ignore-errors\" directive and... cross the fingers."
                        sys.exit(1)
                else:
                    print >> sys.stdout, "Hooray! No failures in backups found!"
        else:
            print >> sys.stdout, ("%s available archive log%s seems healthy." % (len(ha), len(ha) > 1 and 's are' or '' ))


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

        print >> sys.stdout, ("Restoring the SUSE Manager Database using %s strategy" % strategy)

        # Could be database just not cleanly killed
        # In this case great almighty RMAN won't connect at all and just crashes. :-(
        self.do_db_start()
        self.do_db_stop()

        print >> sys.stdout, "Preparing database:\t",
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
        
        print >> sys.stdout, "Restoring from backup:\t",
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario(scenario[strategy], target='rman', dbid=str(dbid))
        
        if stderr:
            roller.stop("failed")
            time.sleep(1)
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr
            
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

        print >> sys.stdout, "Gathering statistics on SUSE Manager database...\t",

        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('gather-stats', owner=self.config.get('db_user', '').upper())

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
        self.vw_check_database_ready("Database must be healthy and running in order to get space overview!");
        stdout, stderr = self.call_scenario('report')
        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr
            return
        
        ora_error = self.has_ora_error(stdout)
        if ora_error:
            raise GateException("Please visit http://%s.ora-code.com/ page to know more details." % ora_error.lower())

        table = [("Tablespace", "Avail (Mb)", "Used (Mb)", "Size (Mb)", "Use %",),]
        for name, free, used, size in [" ".join(filter(None, line.replace("\t", " ").split(" "))).split(" ") 
                                       for line in stdout.strip().split("\n")[2:]]:
            table.append((name, free, used, size, str(int(float(used) / float(size) * 100)),))
        print >> sys.stdout, "\n", TablePrint(table), "\n"


    def do_stats_overview(self, *args, **params):
        """
        Show tables with stale or empty statistics.
        """
        self.vw_check_database_ready("Database must be healthy and running in order to get stats overview!");
        print >> sys.stdout, "Preparing data:\t\t",
        
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('stats', owner=self.config.get('db_user', '').upper())

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
        self.vw_check_database_ready("Database must be healthy and running in order to reclaim the used space!");        

        print >> sys.stdout, "Examining the database...\t",
        roller = Roller()
        roller.start()

        stdout, stderr = self.call_scenario('shrink-segments-advisor')
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
        stdout, stderr = self.call_scenario('recomendations')

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
        Start the SUSE Manager database listener.
        """
        if not 'quiet' in args:
            print >> sys.stdout, "Starting database listener...\t",
            sys.stdout.flush()

        dbstatus = self.get_status()
        if dbstatus.ready:
            if not 'quiet' in args:
                print >> sys.stdout, "Failed"
                print >> sys.stderr, "Error: listener already running."
            return

        ready = False
        stdout, stderr = self.syscall("sudo", None, None, "-u", "oracle", "ORACLE_HOME=" + self.ora_home, self.lsnrctl, "start")
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
        Stop the SUSE Manager database listener.
        @help
        quiet\tSuppress any output.
        """
        if not 'quiet' in args:
            print >> sys.stdout, "Stopping database listener...\t",
            sys.stdout.flush()

        dbstatus = self.get_status()
        if not dbstatus.ready:
            if not 'quiet' in args:
                print >> sys.stdout, "Failed"
                print >> sys.stderr, "Error: listener is not running."
                return

        success = False
        stdout, stderr = self.syscall("sudo", None, None, "-u", "oracle", "ORACLE_HOME=" + self.ora_home, self.lsnrctl, "stop")
        
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
        print >> sys.stdout, "Listener:\t",
        sys.stdout.flush()

        dbstatus = self.get_status()
        print >> sys.stdout, (dbstatus.ready and "running" or "down")
        print >> sys.stdout, "Uptime:\t\t", dbstatus.uptime and dbstatus.uptime or ""
        print >> sys.stdout, "Instances:\t", dbstatus.available
        
        if dbstatus.stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, dbstatus.stderr

        if dbstatus.unknown:
            print >> sys.stderr, "Warning: %s unknown instance%s." % (dbstatus.unknown, dbstatus.unknown > 1 and 's' or '')
        if not dbstatus.available:
            print >> sys.stderr, "Critical: No available instances found!"


    def do_listener_restart(self, *args, **params):
        """
        Restart SUSE Manager database listener.
        """
        print >> sys.stdout, "Restarting listener...",
        sys.stdout.flush()

        dbstatus = self.get_status()
        if dbstatus.ready:
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

        dbstatus = self.get_status()
        if dbstatus.ready:
            roller.stop('failed')
            time.sleep(1)
            raise GateException("Error: listener is already running")
        else:
            self.do_listener_start('quiet')

        roller.stop('done')
        time.sleep(1)

        print >> sys.stdout, "Starting core...\t",
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        stdout, stderr = self.syscall("sudo", None, None, "-u", "oracle", self.ora_home + "/bin/dbstart")
        roller.stop('done')
        time.sleep(1)

        return
    
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

        dbstatus = self.get_status()
        if dbstatus.ready:
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

        if stderr:
            print >> sys.stderr, "\nError dump:"
            print >> sys.stderr, stderr + "\n"


    def do_db_status(self, *args, **params):
        """
        Display SUSE Manager database runtime status.
        """
        print >> sys.stdout, "Checking database core...\t",
        sys.stdout.flush()

        dbstatus = self.get_db_status()
        if dbstatus.ready:
            print >> sys.stdout, "online"
        else:
            print >> sys.stdout, "offline"
            


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
        ora_error = self.has_ora_error(stdout)
        if ora_error:
            raise GateException("Please visit http://%s.ora-code.com/ page to know more details." % ora_error.lower())

        for tname, tsize in filter(None, [filter(None, line.replace("\t", " ").split(" ")) for line in stdout.split("\n")]):
            table.append((tname, ('%.2fK' % round(float(tsize) / 1024.)),))
            total += float(tsize)
        table.append(('', '',))
        table.append(('Total', ('%.2fM' % round(total / 1024. / 1024.))))

        if table:
            print >> sys.stdout, "\n", TablePrint(table), "\n"

        if stderr:
            print >> sys.stderr, "Error dump:"
            print >> sys.stderr, stderr
            raise Exception("Unhandled underlying error.")


    def do_db_check(self, *args, **params):
        """
        Check full connection to the database.
        """
        print >> sys.stdout, "Checking connection:\t",
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
        print >> sys.stdout, "Checking SUSE Manager database backend\n"

        # Set data table autoextensible.
        stdout, stderr = self.call_scenario('cnf-get-noautoext')
        if stderr:
            print >> sys.stderr, "Autoextend check error:"
            print >> sys.stderr, stderr
            raise GateException("Unable continue system check")

        if stdout:
            print >> sys.stdout, "Autoextensible:\tOff"
            scenario = []
            [scenario.append("alter database datafile '%s' autoextend on;" % fname) for fname in stdout.strip().split("\n")]
            self.syscall("sudo", self.get_scenario_template().replace('@scenario', '\n'.join(scenario)), 
                         None, "-u", "oracle", "/bin/bash")
            print >> sys.stdout, "%s table%s has been autoextended" % (len(scenario), len(scenario) > 1 and 's' or '')
        else:
            print >> sys.stdout, "Autoextensible:\tYes"

        # Turn on archivelog.
        #
        if 'force-archivelog-off' in args:
            if self.get_archivelog_mode():
                self.set_archivelog_mode(status=False)
            else:
                print >> sys.stdout, "Archivelog mode is not used."
        else:
            if not self.get_archivelog_mode():
                self.set_archivelog_mode(True)
                if not self.get_archivelog_mode():
                    print >> sys.stderr, "No archive log"
                else:
                    print >> sys.stdout,  "Database is now running in archivelog mode."
            else:
                print >> sys.stdout, "Archivelog:\tYes"

        # Free space on the storage.
        #
        # TBD

        print >> sys.stdout, "\nFinished\n"


    def set_archivelog_mode(self, status=True):
        """
        Set archive log mode status.
        """
        print >> sys.stdout, ("Turning %s archivelog mode...\t" % (status and 'on' or 'off')),
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
                dbid = long(stdout.split("\n")[-1])
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
                dbidkey, dbid = map(lambda el:el.strip(), line.split('=', 1))
                if dbid:
                    try:
                        dbid = long(dbid)
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
            ftkn = filter(None, line.split(" "))[0]
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
            print >> sys.stderr, "Backup information check failure:"
            print >> sys.stderr, stderr
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
            print >> sys.stderr, "Archive log information check failure:"
            print >> sys.stderr, stderr
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
            for line in map(lambda elm:elm.strip(), bkpsout.split("crosschecked")):
                if not line.startswith("backup piece"):
                    continue
                obj_raw = line.split("\n")[:2]
                if len(obj_raw) == 2:
                    status = obj_raw[0].strip().split(" ")[-1].replace("'", '').lower()
                    data = dict(filter(None, map(lambda elm:"=" in elm and tuple(elm.split("=", 1)) or None, filter(None, obj_raw[-1].split(" ")))))
                    hinfo = HandleInfo(status, handle=data['handle'], recid=data['RECID'], stamp=data['STAMP'])
                    if hinfo.availability == 'available':
                        healthy_backups.append(hinfo)
                    else:
                        failed_backups(hinfo)

        # Check failed archive logs
        if arlgout:
            for archline in map(lambda elm:elm.strip(), arlgout.split("validation", 1)[-1].split("Crosschecked")[0].split("validation")):
                obj_raw = archline.split("\n")
                if len(obj_raw) == 2:
                    status = obj_raw[0].split(" ")[0]
                    data = dict(filter(None, map(lambda elm:'=' in elm and tuple(elm.split('=', 1)) or None, obj_raw[1].split(" "))))
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
            print >> sys.stderr, "Backup information listing failure:"
            print >> sys.stderr, stderr
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
                tkn = filter(None, line.replace("\t", " ").split(" "))
                info[tkn[5]] = BackupInfo(tkn[0], tkn[5], tkn[-1])
                idx.append(tkn[5])

        return [info[bid] for bid in reversed(sorted(idx))]


    def vw_check_database_ready(self, message, output_shift=1):
        """
        Check if database is ready. Otherwise crash with the given message.
        """
        print >> sys.stdout, "Checking the database:" + ("\t" * output_shift),
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

        print >> sys.stderr, "ERROR:", stderr

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
            print >> sys.stderr, "WARNING: Reserved space for the backup is smaller than available disk space. Adjusting."
            if not self.autoresize_available_archive(target_fds):
                print >> sys.stderr, "WARNING: Could not adjust system for backup reserved space!"
            else:
                print >> sys.stdout, "INFO: System settings for the backup recovery area has been altered successfully."


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
