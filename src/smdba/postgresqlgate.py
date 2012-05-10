# PostgreSQL gate
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

import sys
import os
import time
import shutil
import tempfile
import utils



class PgSQLGate(BaseGate):
    """
    Gate for PostgreSQL database tools.
    """
    NAME = "postgresql"


    def __init__(self, config):
        self.config = config or {}
        self._get_sysconfig()
        self._get_pg_data()
        if self._get_db_status():
            self._get_pg_config()


    # Utils
    def check(self):
        """
        Check system requirements for this gate.
        """
        msg = None
        if os.popen('/usr/bin/postmaster --version').read().strip().split(' ')[-1] < '9.1':
            raise GateException("Core component is too old version.")
        elif not os.path.exists("/etc/sysconfig/postgresql"):
            raise GateException("Custom core component? Please strictly use SUSE components only!")
        elif not os.path.exists("/usr/bin/psql"):
            msg = 'operations'
        elif not os.path.exists("/usr/bin/postmaster"):
            msg = 'core'
        elif not os.path.exists("/usr/bin/pg_ctl"):
            msg = 'control'
        elif not os.path.exists("/usr/bin/pg_basebackup"):
            msg = 'backup'
        
        if msg:
            raise GateException("Cannot find required %s component." % msg)

        return True


    def _get_sysconfig(self):
        """
        Read the system config for the postgresql.
        """
        for line in filter(None, map(lambda line:line.strip(), open('/etc/sysconfig/postgresql').readlines())):
            if line.startswith('#'):
                continue
            try:
                k, v = line.split("=", 1)
                self.config['sysconfig_' + k] = v
            except:
                print >> sys.stderr, "Cannot parse line", line, "from sysconfig."


    def _get_db_status(self):
        """
        Return True if DB is running, False otherwise.
        """
        status = False
        pid_file = self.config.get('pcnf_pg_data', '') + '/postmaster.pid'
        if os.path.exists(pid_file):
            if os.path.exists('/proc/' + open(pid_file).readline().strip()):
                status = True

        return status


    def _get_pg_data(self):
        """
        PostgreSQL data dir from sysconfig.
        """
        for line in open("/etc/sysconfig/postgresql").readlines():
            if line.startswith('POSTGRES_DATADIR'):
                self.config['pcnf_pg_data'] = os.path.expanduser(line.strip().split('=', 1)[-1].replace('"', ''))

        if not os.path.exists(self.config.get('pcnf_pg_data', '')):
            raise GateException('Cannot find core component tablespace on disk')


    def _get_pg_config(self):
        """
        Get entire PostgreSQL configuration.
        """
        stdout, stderr = self.syscall("sudo", self.get_scenario_template(target='psql')
                                      .replace('@scenario', 'show all'),
                                      None, "-u", "postgres", "/bin/bash")
        if stdout:
            for line in stdout.strip().split("\n")[2:]:
                try:
                    k, v = map(lambda line:line.strip(), line.split('|')[:2])
                    self.config['pcnf_' + k] = v
                except:
                    print >> sys.stdout, "Cannot parse line:", line
        else:
            print >> sys.stderr, stderr
            raise Exception("Underlying error: unable get backend configuration.")


    def _bt_to_mb(self, v):
        """
        Bytes to megabytes.
        """
        return int(round(v / 1024. / 1024.))


    def _cleanup_pids(self):
        """
        Cleanup PostgreSQL garbage in /tmp
        """
        for f in os.listdir('/tmp'):
            if f.startswith('.s.PGSQL.'):
                os.unlink('/tmp/' + f)


    def _get_conf(self, conf_path):
        """
        Get a PostgreSQL config file into a dictionary.
        """
        if not os.path.exists(conf_path):
            raise GateException("Cannot open config at \"%s\"." % conf_path)

        conf = {}
        for line in open(conf_path).readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                k, v = [el.strip() for el in line.split('#')[0].strip().split('=', 1)]
                conf[k] = v
            except Exception, ex:
                raise GateException("Cannot parse line [%s] in %s." % (line, conf_path))

        return conf


    def _write_conf(self, conf_path, *table, **data):
        """
        Write conf data to the file.
        """
        backup = None
        if os.path.exists(conf_path):
            pref = '-'.join([str(el).zfill(2) for el in time.localtime()][:6])
            conf_path_new = conf_path.split(".")
            conf_path_new = '.'.join(conf_path_new[:-1]) + "." + pref + "." + conf_path_new[-1]
            os.rename(conf_path, conf_path_new)
            backup = conf_path_new

        if data or table:
            cfg = open(conf_path, 'w')
            if data and not table:
                [cfg.write('%s = %s\n' % items) for items in data.items()]
            elif table and not data:
                [cfg.write('\t'.join(items) + "\n") for items in table]
            cfg.close()
        else:
            raise IOError("Cannot write two different types of config into the same file!")

        return backup


    # Commands        
    def do_db_start(self, **args):
        """
        Start the SUSE Manager Database.
        """
        print >> sys.stdout, "Starting core...\t",
        sys.stdout.flush()
        #roller = Roller()
        #roller.start()

        if self._get_db_status():
            print >> sys.stdout, "failed"
            #roller.stop('failed')
            time.sleep(1)
            return

        # Cleanup first
        self._cleanup_pids()

        # Start the db
        if not os.system("sudo -u postgres /usr/bin/pg_ctl start -s -w -p /usr/bin/postmaster -D %s -o %s" 
                         % (self.config['pcnf_pg_data'], self.config.get('sysconfig_POSTGRES_OPTIONS', ''))):
            print >> sys.stdout,  "done"
        else:
            print >> sys.stderr, "failed"

        #roller.stop('done')
        time.sleep(1)


    def do_db_stop(self, **args):
        """
        Stop the SUSE Manager Database.
        """
        print >> sys.stdout, "Stopping core...\t",
        sys.stdout.flush()

        if not self._get_db_status():
            print >> sys.stdout, "failed"
            #roller.stop('failed')
            time.sleep(1)
            return

        # Stop the db
        if not self.config.get('pcnf_data_directory'):
            raise GateException("Cannot find data directory.")

        if not os.system("sudo -u postgres /usr/bin/pg_ctl stop -s -D %s -m fast" % self.config.get('pcnf_data_directory', '')):
            print >> sys.stdout, "done"
        else:
            print >> sys.stderr, "failed"

        # Cleanup
        self._cleanup_pids()


    def do_db_status(self, **args):
        """
        Show database status.
        """
        print 'Database is', self._get_db_status() and 'online' or 'offline'


    def do_space_tables(self, **args):
        """
        Show space report for each table.
        """
        stdout, stderr = self.call_scenario('pg-tablesizes', target='psql')

        if stdout:
            t_index = []
            t_ref = {}
            t_total = 0
            longest = 0
            for line in stdout.strip().split("\n")[2:]:
                line = filter(None, map(lambda el:el.strip(), line.split('|')))
                if len(line) == 3:
                    t_name, t_size_pretty, t_size = line[0], line[1], int(line[2])
                    t_ref[t_name] = t_size_pretty
                    t_total += t_size
                    t_index.append(t_name)

                    longest = len(t_name) > longest and len(t_name) or longest

            t_index.sort()

            table = [('Table', 'Size',)]
            for name in t_index:
                table.append((name, t_ref[name],))
            table.append(('', '',))
            table.append(('Total', ('%.2f' % round(t_total / 1024. / 1024)) + 'M',))
            print >> sys.stdout, "\n", TablePrint(table), "\n"

        if stderr:
            print >> sys.stderr, stderr
            raise GateException("Unhandled underlying error occurred, see above.")


    def _get_partition(self, fdir):
        """
        Get partition of the directory.
        """
        return os.popen("df -lP %s | tail -1 | cut -d' ' -f 1" % fdir).read().strip()
        

    def do_space_overview(self, **args):
        """
        Show database space report.
        """
        # Not exactly as in Oracle, this one looks where PostgreSQL is mounted
        # and reports free space.

        if not self._get_db_status():
            raise GateException("Database must be running.")

        # Get current partition
        partition = self._get_partition(self.config['pcnf_data_directory'])

        # Build info
        class Info:
            fs_dev = None
            fs_type = None
            used = None
            available = None
            used_prc = None
            mountpoint = None

        info = Info()
        for line in os.popen("df -T").readlines()[1:]:
            line = line.strip()
            if not line.startswith(partition):
                continue
            line = filter(None, line.split(" "))
            info.fs_dev = line[0]
            info.fs_type = line[1]
            info.used = int(line[2]) * 1024 # Bytes
            info.available = int(line[4]) * 1024 # Bytes
            info.used_prc = line[5]
            info.mountpoint = line[6]

            break
        

        # Get database sizes
        stdout, stderr = self.syscall("sudo", self.get_scenario_template(target='psql').replace('@scenario', 
                                                                                                'select pg_database_size(datname), datname from pg_database;'),
                                      None, "-u", "postgres", "/bin/bash")
        overview = [('Tablespace', 'Size (Mb)', 'Avail (Mb)', 'Use %',)]
        for line in stdout.split("\n")[2:]:
            line = filter(None, line.strip().replace('|', '').split(" "))
            if len(line) != 2:
                continue
            d_size = int(line[0])
            d_name = line[1]
            d_size_available = (info.available - d_size)
            overview.append((d_name, self._bt_to_mb(d_size),
                             self._bt_to_mb(d_size_available),
                             '%.3f' % round((float(d_size) / float(d_size_available) * 100), 3)))

        print >> sys.stdout, "\n", TablePrint(overview), "\n"


    def do_space_reclaim(self, **args):
        """
        Free disk space from unused object in tables and indexes.
        """
        print >> sys.stdout, "Examining core...\t",
        sys.stdout.flush()

        #roller = Roller()
        #roller.start()        

        if not self._get_db_status():
            roller.stop('failed')
            time.sleep(1)
            #print >> sys.stderr, "failed"
            raise GateException("Database must be online.")

        print >> sys.stderr, "finished"
        #roller.stop('done')
        time.sleep(1)

        operations = [
            ('Analyzing database', 'vacuum analyze;'),
            ('Reclaiming space', 'cluster;'),
            ]

        for msg, operation in operations:
            print >> sys.stdout, "%s...\t" % msg,
            sys.stdout.flush()
            #roller = Roller()
            #roller.start()

            stdout, stderr = self.syscall("sudo", self.get_scenario_template(target='psql').replace('@scenario', operation),
                                          None, "-u", "postgres", "/bin/bash")
            if stderr:
                #roller.stop('failed')
                #time.sleep(1)
                print >> sys.stderr, "failed"
                sys.stdout.flush()
                print >> sys.stderr, stderr
                raise GateException("Unhandled underlying error occurred, see above.")
            
            else:
                #roller.stop('done')
                #time.sleep(1)
                print >> sys.stdout, "done"
                sys.stdout.flush()
                #print stdout

    def _get_tablespace_size(self, path):
        """
        Get tablespace size in bytes.
        """
        return long(os.popen('/usr/bin/du -bc %s' % path).readlines()[-1].strip().replace('\t', ' ').split(' ')[0])


    def _rst_get_backup_root(self, path):
        """
        Get root of the backup.
        NOTE: Now won't work with multiple backups.
        """
        path = os.path.normpath(path)
        found = None
        fpath = os.listdir(path)
        if 'backup_label' in fpath: # XXX: Add search by label too for multiple backups?
            return path
        for f in fpath:
            f = path + "/" + f
            if os.path.isdir(f):
                found = self._rst_get_backup_root(f)
                if found:
                    break;

        return found


    def _rst_save_current_cluster(self):
        """
        Save current tablespace
        """
        old_data_dir = os.path.dirname(self.config['pcnf_pg_data']) + '/data.old'
        if not os.path.exists(old_data_dir):
            os.mkdir(old_data_dir)
            print >> sys.stdout, "Created \"%s\" directory." % old_data_dir

        print >> sys.stdout, "Moving broken cluster:\t ",
        sys.stdout.flush()
        roller = Roller()
        roller.start()
        suffix = '-'.join([str(el).zfill(2) for el in time.localtime()][:6])
        destination_tar = old_data_dir + "/data." + suffix + ".tar.gz"
        tar_command = '/bin/tar -czPf %s %s 2>/dev/null' % (destination_tar, self.config['pcnf_pg_data'])
        os.system(tar_command)
        roller.stop("finished")
        time.sleep(1)
        sys.stdout.flush()

    def _rst_shutdown_db(self):
        """
        Gracefully shutdown the database.
        """
        if self._get_db_status():
            self.do_db_stop()
            self.do_db_status()
            if self._get_db_status():
                print >> sys.stderr, "Error: Unable to stop database."
                sys.exit(1)


    def _rst_replace_new_backup(self, backup_dst):
        """
        Replace new backup.
        """
        # Archive into a tgz backup and place it near the cluster
        print >> sys.stdout, "Restoring from backup:\t ",
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        destination_tar = os.path.dirname(self.config['pcnf_pg_data']) + "/backup.tar.gz"
        tar_command = '/bin/tar -czPf %s %s 2>/dev/null' % (destination_tar, backup_dst)
        os.system(tar_command)
        #print tar_command
        
        roller.stop("finished")
        time.sleep(1)

        # Remove cluster in general
        print >> sys.stdout, "Remove broken cluster:\t",
        sys.stdout.flush()
        shutil.rmtree(self.config['pcnf_pg_data'])
        print >> sys.stdout, "finished"
        sys.stdout.flush()

        # Unarchive cluster
        print >> sys.stdout, "Unarchiving new backup:\t ",
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        temp_dir = tempfile.mkdtemp()
        tar_command = '/bin/tar xf %s --directory=%s 2>/dev/null' % (destination_tar, temp_dir)
        os.system(tar_command)
        #print tar_command

        roller.stop("finished")
        time.sleep(1)

        print >> sys.stdout, "Restore cluster:\t",
        backup_root = self._rst_get_backup_root(temp_dir)
        mv_command = '/bin/mv %s %s' % (backup_root, os.path.dirname(self.config['pcnf_pg_data']) + "/data")
        os.system(mv_command)
        #print mv_command
        print >> sys.stdout, "finished"
        sys.stdout.flush()        


    def do_backup_restore(self, *opts, **args):
        """
        Restore the SUSE Manager Database from backup.
        """
        # This is the ratio of compressing typical PostgreSQL cluster tablespace
        ratio = 0.134

        backup_dst, backup_on = self.do_backup_status('--silent')
        if not backup_on:
            print >> sys.stderr, "No backup snapshots are available."
            sys.exit(1)

        # Check if we have enough space to fit enough copy of the tablespace
        curr_ts_size = self._get_tablespace_size(self.config['pcnf_pg_data'])
        bckp_ts_size = self._get_tablespace_size(backup_dst)
        disk_size = self._get_partition_size(self.config['pcnf_pg_data'])

        print >> sys.stdout, "Current cluster size:\t", self.size_pretty(curr_ts_size)
        print >> sys.stdout, "Backup size:\t\t", self.size_pretty(bckp_ts_size)
        print >> sys.stdout, "Current disk space:\t", self.size_pretty(disk_size)
        print >> sys.stdout, "Predicted space:\t", self.size_pretty(disk_size - (curr_ts_size * ratio) - bckp_ts_size)

        # At least 1GB free disk space required *after* restore from the backup
        if disk_size - curr_ts_size - bckp_ts_size < 0x40000000:
            print >> sys.stderr, "At least 1GB free disk space required after backup restoration."
            sys.exit(1)

        # Requirements were met at this point.
        #
        # Shutdown the db
        self._rst_shutdown_db()

        # Save current tablespace
        self._rst_save_current_cluster()

        # Replace with new backup
        self._rst_replace_new_backup(backup_dst)
        self.do_db_start()


    def do_backup_hot(self, *opts, **args):
        """
        Perform host database backup.
        @help
        --enable\tEnable or disable hot backups. Values: on | off | purge
        --destination\tDestination directory of the backup.\n
        """

        # Part for the auto-backups
        #--source\tSource path of WAL entry.\n
        #Example:
        #--autosource=%p --destination=/root/of/your/backups\n
        #NOTE: All parameters above are used automatically!\n

        if 'destination' not in args.keys():
            raise GateException("Where I have to put backups?")

        if 'enable' in args.keys():
            self._perform_enable_backups(**args)

        if 'source' in args.keys():
            # Copy xlog entry
            self._perform_archive_operation(**args)
        

    def _perform_enable_backups(self, **args):
        """
        Turn backups on or off.
        """
        enable = args.get('enable', 'off')
        conf_path = self.config['pcnf_pg_data'] + "/postgresql.conf"
        conf = self._get_conf(conf_path)

        if enable == 'on':
            # Enable backups
            if not self._get_db_status():
                self.do_db_start()
            if not self._get_db_status():
                raise GateException("Cannot start the database!")

            if not os.path.exists(args.get('destination')):
                os.system('sudo -u postgres /usr/bin/pg_basebackup -D %s -Fp -c fast -x -v -P' % (args['destination']))

            cmd = "'" + args['__console_location'] +" backup-hot auto --backend=postgresql --source=\"%p\" --destination=\"" + args['destination'] + "/%f\"'"
            if conf.get('archive_command', '') != cmd:
                conf['archive_command'] = cmd
                conf_bk = self._write_conf(conf_path, **conf)
                self._restart_db()
        else:
            # Disable backups
            if enable == 'purge' and os.path.exists(args['destination']):
                shutil.rmtree(args['destination'])

            cmd = "'/bin/true'"
            if conf.get('archive_command', '') != cmd:
                conf['archive_command'] = cmd
                conf_bk = self._write_conf(conf_path, **conf)
                self._restart_db()


    def _restart_db(self):
        """
        Restart the entire db.
        """
        if self._get_db_status():
            self.do_db_stop()
        self.do_db_start()
        self.do_db_status()


    def _perform_archive_operation(self, **args):
        """
        Performs an archive operation.
        """
        if not args.get('source'):
            raise GateException("Source file was not specified!")
        elif not os.path.exists(args.get('source')):
            raise GateException("File \"%s\" does not exists." % args.get('source'))
        elif os.path.exists(args.get('destination')):
            raise GateException("Destination file \"%s\"already exists." % args.get('destination'))
        shutil.copy2(args.get('source'), args.get('destination'))


    def do_backup_status(self, *opts, **args):
        """
        Show backup status.
        """
        backup_dst = ""
        backup_on = False
        conf_path = self.config['pcnf_pg_data'] + "/postgresql.conf"
        conf = self._get_conf(conf_path)
        cmd = self._get_conf(conf_path).get('archive_command', '').split(" ")
        for line in cmd:
            if line.startswith('--destination'):
                backup_dst = os.path.dirname(line.split('=', 1)[-1].replace('"', '').replace("'", ''))
                backup_on = os.path.exists(backup_dst)

        backup_last_transaction = None
        if backup_dst:
            for fh in os.listdir(backup_dst + "/pg_xlog"):
                mtime = os.path.getmtime(backup_dst + "/pg_xlog/" + fh)
                if mtime > backup_last_transaction:
                    backup_last_transaction = mtime

        space_usage = None
        if backup_dst:
            partition = self._get_partition(backup_dst)
            for line in os.popen("df -T").readlines()[1:]:
                line = line.strip()
                if not line.startswith(partition):
                    continue
                space_usage = (filter(None, line.split(' '))[5] + '').replace('%', '')

        if not '--silent' in opts:
            print >> sys.stdout, "Backup status:\t\t", (backup_on and 'ON' or 'OFF')
            print >> sys.stdout, "Destination:\t\t", (backup_dst or '--')
            print >> sys.stdout, "Last transaction:\t", backup_last_transaction and time.ctime(backup_last_transaction) or '--'
            print >> sys.stdout, "Space available:\t", space_usage and str((100 - int(space_usage))) + '%' or '--'
        else:
            return backup_dst, backup_on


    def _get_partition_size(self, path):
        """
        Get a size of the partition, where path belongs to."
        """
        return long((filter(None, (os.popen("df -TB1 %s" % path).readlines()[-1] + '').split(' '))[4] + '').strip())
        

    def do_system_check(self, **args):
        """
        Common backend healthcheck.
        """
        # Check enough space

        # Check hot backup setup and clean it up automatically
        conf_path = self.config['pcnf_pg_data'] + "/postgresql.conf"
        conf = self._get_conf(conf_path)
        changed = False

        #
        # Setup postgresql.conf
        #

        # WAL should be at least archive.
        if not conf.get('wal_level') or conf.get('wal_level') == 'minimal':
            conf['wal_level'] = 'archive'

        # WAL senders at least 5
        if not conf.get('max_wal_senders') or conf.get('max_wal_senders') < '5':
            conf['max_wal_senders'] = 5
            changed = True

        # WAL keep segments must be non-zero
        if conf.get('wal_keep_segments', '0') == '0':
            conf['wal_keep_segments'] = 300
            changed = True

        # Should run in archive mode
        if conf.get('archive_mode', 'off') != 'on':
            conf['archive_mode'] = 'on'
            changed = True

        # Stub
        if conf.get('archive_command', '') != "'/bin/true'":
            conf['archive_command'] = "'/bin/true'"
            changed = True

        #
        # Setup pg_hba.conf
        # Format is pretty specific :-)
        #
        hba_changed = False
        pg_hba_cnf_path = self.config['pcnf_pg_data'] + "/pg_hba.conf"
        pg_hba_conf = []
        for line in open(pg_hba_cnf_path).readlines():
            line = line.strip()
            if not line or line.startswith('#'): continue
            pg_hba_conf.append(filter(None, line.replace("\t", " ").split(' ')))

        replication_cfg = ['local', 'replication', 'postgres', 'peer']

        if not replication_cfg in pg_hba_conf:
            pg_hba_conf.append(replication_cfg)
            hba_changed = True

        #
        # Commit the changes
        #
        if changed or hba_changed:
            print >> sys.stdout, "INFO: Database needs to be restarted."
            if changed:
                conf_bk = self._write_conf(conf_path, **conf)
                if conf_bk:
                    print >> sys.stdout, "INFO: Wrote new general configuration. Backup as", conf_bk

            # hba save
            if hba_changed:
                conf_bk = self._write_conf(pg_hba_cnf_path, *pg_hba_conf)
                if conf_bk:
                    print >> sys.stdout, "INFO: Wrote new client auth configuration. Backup as", conf_bk

            # Restart
            if self._get_db_status():
                self.do_db_stop()
            self.do_db_start()
            self.do_db_status()
        else:
            print >> sys.stdout, "INFO: No changes required."

        print >> sys.stdout, "System check finished"

        return True



def getGate(config):
    """
    Get gate to the database engine.
    """
    return PgSQLGate(config)
