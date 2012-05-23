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
                    print >> sys.stderr, "No backup snapshots are available."
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

        owner = utils.get_path_owner(params.get('backup-dir'))
        if owner.user != 'oracle':
            raise Exception("\tDirectory \"%s\" does not have proper permissions!" % params.get('backup-dir'))

        if not self.get_archivelog_mode():
            raise GateException("Archivelog is not turned on.\n\tPlease shutdown SUSE Manager and run system-check first!")

        print >> sys.stdout, "Backing up the database:\t",
        roller = Roller()
        roller.start()
        stdout, stderr = self.call_scenario('rman-hot-backup', target='rman', backupdir=params.get('backup-dir'))

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
        dbstatus = self.get_db_status()
        if dbstatus.ready:
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

        stdout, stderr = self.call_scenario('rman-hot-backup.scn', target='rman', backupdir=params.get('backup-dir'))

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


    def do_stats_refresh(self, *args, **params):
        """
        Gather statistics on SUSE Manager Database database objects.
        """
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
        stdout, stderr = self.call_scenario('report')
        table = [("Tablespace", "Size (Mb)", "Used (Mb)", "Avail (Mb)", "Use %",),]
        for name, free, used, size in [" ".join(filter(None, line.replace("\t", " ").split(" "))).split(" ") 
                                       for line in stdout.strip().split("\n")[2:]]:
            table.append((name, free, used, size, str(int(float(used) / float(size) * 100)),))

        print >> sys.stdout, "\n", TablePrint(table), "\n"


    def do_stats_overview(self, *args, **params):
        """
        Show tables with stale or empty statistics.
        """
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
        
        dbstatus = self.get_db_status()
        if not dbstatus.ready:
            raise Exception("Database is not running.")

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
        Start the SUSE Manager Database listener.
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
        Stop the SUSE Manager Database listener.
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
        Restart SUSE Database Listener.
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
        Get SUSE Database running status.
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
        table = [('Table', 'Size',)]
        total = 0
        stdout, stderr = self.call_scenario('tablesizes', user=self.config.get('db_user', '').upper())
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

        sid = self.config.get("db_name", "")
        #status.stdout, status.stderr = self.syscall(self.lsnrctl, None, None, "status")
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
        print >> sys.stdout, "Checking SUSE Manager Database backend\n"

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
            destination = os.environ['ORACLE_BASE'] + "/oradata/" + self.config.get("db_name") + "/archive"
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



def getGate(config):
    """
    Get gate to the database engine.
    """
    return OracleGate(config)
