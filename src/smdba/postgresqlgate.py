# coding: utf-8
"""
PostgreSQL gate
"""
# These lints cannot be touched unless tests are made!
# pylint: disable=R0915,R0912,W0511,W0123,R0914,C0321

import sys
import os
import re
import pwd
import grp
import time
import shutil
import tempfile
import stat

from smdba.basegate import BaseGate, GateException
from smdba.roller import Roller
from smdba.utils import TablePrint, get_path_owner, eprint


class PgBackup:
    """
    PostgreSQL backup utilities wrapper.
    """
    # NOTE: First attempt to restructure
    #       all the wrapping behind bunch of the utilities, needed for
    #       the backup.

    DEFAULT_PG_DATA = "/var/lib/pgsql/data/"
    PG_ARCHIVE_CLEANUP = "/usr/bin/pg_archivecleanup"

    def __init__(self, target_path, pg_data=None):
        if not os.path.exists(PgBackup.PG_ARCHIVE_CLEANUP):
            raise Exception("The utility pg_archivecleanup was not found on the path.")

        self.target_path = target_path
        self.pg_data = pg_data or PgBackup.DEFAULT_PG_DATA
        self.pg_xlog = os.path.join(self.pg_data, "pg_xlog")

    @staticmethod
    def _get_latest_restart_filename(path):
        checkpoints = []
        history = []
        restart_filename = None

        for fname in os.listdir(path):
            if not stat.S_ISREG(os.stat(os.path.join(path, fname)).st_mode):
                continue
            if fname.endswith(".backup"):
                checkpoints.append(fname)
            if fname.endswith(".history"):
                history.append(fname)

        checkpoints = sorted(checkpoints)
        history = sorted(history)
        if checkpoints:
            restart_filename = checkpoints.pop(len(checkpoints) - 1)

        if history:
            history.pop(len(history) - 1)

        return checkpoints, history, restart_filename

    def cleanup_backup(self):
        """
        Cleans up the whole backup.
        This method depends on pg_archivecleanup external utility which removes
        older WAL files from PostgreSQL archives.
        """
        checkpoints, _, restart_filename = self._get_latest_restart_filename(self.target_path)
        for obsolete_bkp_chkpnt in checkpoints:
            os.unlink(os.path.join(self.target_path, obsolete_bkp_chkpnt))

        if restart_filename:
            os.system("%s %s %s" % (PgBackup.PG_ARCHIVE_CLEANUP, self.target_path, restart_filename))


class PgTune:
    """
    PostgreSQL tuning.
    """
    # NOTE: This is default Alpha implementation for SUSE Manager specs.
    #       With a time it going to get more smart and dynamic.

    def __init__(self, max_connections):
        self.max_connections = max_connections
        self.config = {}

    @staticmethod
    def get_total_memory() -> int:
        """
        Get machine total memory.

        :returns total memory
        """
        total_memory = 0
        try:
            total_memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        except Exception:
            pass

        return total_memory

    @staticmethod
    def bin_rnd(value: int) -> int:
        """
        Binary rounding.
        Keep 4 significant bits, truncate the rest.

        :param value: a float
        :returns binary round value
        """
        mbt = 1
        while value > 0x10:
            value = int(value / 2)
            mbt *= 2

        return mbt * value

    @staticmethod
    def to_mb(value: int) -> str:
        """
        Convert to megabytes human-readable string.

        :param value: bytes
        :return:
        """
        return str(int(value / 0x400)) + 'MB'

    def estimate(self):
        """
        Estimate the data.
        """

        kbt = 0x400
        mbt = kbt * 0x400

        mem = self.get_total_memory()
        if not mem:
            raise Exception("Cannot get total memory of this system")

        mem = int(mem / kbt)
        if mem < 0xff * kbt:
            raise Exception("This is a low memory system and is not supported!")

        self.config['shared_buffers'] = self.to_mb(self.bin_rnd(mem / 4))
        self.config['effective_cache_size'] = self.to_mb(self.bin_rnd(mem * 3 / 4))
        self.config['work_mem'] = self.to_mb(self.bin_rnd(mem / self.max_connections))

        # No more than 1GB
        if (mem / 0x10) > mbt:
            maintenance_work_mem = mbt
        else:
            maintenance_work_mem = mem / 0x10
        self.config['maintenance_work_mem'] = self.to_mb(self.bin_rnd(maintenance_work_mem))

        pg_version = [int(v_el) for v_el in os.popen(r"psql --version | sed -e 's/.*\s//g'").read().split('.')]
        if pg_version < [9, 6, 0]:
            self.config['checkpoint_segments'] = 8
        else:
            self.config['max_wal_size'] = self.to_mb(0x60000)

        self.config['checkpoint_completion_target'] = '0.7'
        self.config['wal_buffers'] = self.to_mb(0x200 * 8)
        self.config['constraint_exclusion'] = 'off'
        self.config['max_connections'] = self.max_connections
        self.config['cpu_tuple_cost'] = '0.5'

        return self


class PgSQLGate(BaseGate):
    """
    Gate for PostgreSQL database tools.
    """
    NAME = "postgresql"

    def __init__(self, config):
        BaseGate.__init__(self)
        self.config = config or {}
        self._get_sysconfig()
        self._get_pg_data()

        self._pid_file = os.path.join(self.config.get('pcnf_pg_data', ''), 'postmaster.pid')
        self._with_systemd = os.path.exists('/usr/bin/systemctl')

        if self._get_db_status():
            self._get_pg_config()

    # Utils
    def check(self):
        """
        Check system requirements for this gate.
        """
        msg = None
        minversion = [9, 6]
        pg_version = os.popen('/usr/bin/postmaster --version').read().strip().split(' ')[-1].split('.')

        if int(pg_version[0]) < minversion[0] or (int(pg_version[0]) == minversion[0] and int(pg_version[1]) < minversion[1]):
            raise GateException("Core component is too old version.")

        if not os.path.exists("/etc/sysconfig/postgresql"):
            raise GateException("Custom database component? Please strictly use SUSE components only!")

        if not os.path.exists("/usr/bin/psql"):
            msg = 'operations'
        elif not os.path.exists("/usr/bin/postmaster"):
            msg = 'database'
        elif not os.path.exists("/usr/bin/pg_ctl"):
            msg = 'control'
        elif not os.path.exists("/usr/bin/pg_basebackup"):
            msg = 'backup'
        if msg:
            raise GateException("Cannot find required %s component." % msg)

        # Prevent running this tool within the PostgreSQL data directory
        # See bsc#1024058 for details
        if self.config["pcnf_pg_data"].strip('/') in os.path.abspath("."):
            raise GateException("Please do not call SMDBA inside the '{0}' directory.".format(os.path.abspath(".")))

        return True

    def _get_sysconfig(self):
        """
        Read the system config for the postgresql.
        """
        for line in filter(None, map(lambda line: line.strip(), open('/etc/sysconfig/postgresql').readlines())):
            if line.startswith('#'):
                continue
            try:
                key, val = line.split("=", 1)
                self.config['sysconfig_' + key] = val
            except Exception as ex:
                eprint("Cannot parse line", line, "from sysconfig.")
                eprint(ex)

    def _get_db_status(self):
        """
        Return True if DB is running, False otherwise.
        """
        status = False
        if os.path.exists(self._pid_file):
            if os.path.exists(os.path.join('/proc', open(self._pid_file).readline().strip())):
                status = True

        return status

    def _get_pg_data(self):
        """
        PostgreSQL data dir from sysconfig.
        """
        for line in open("/etc/sysconfig/postgresql").readlines():
            if line.startswith('POSTGRES_DATADIR'):
                self.config['pcnf_pg_data'] = os.path.expanduser(line.strip().split('=', 1)[-1].replace('"', ''))

        if self.config.get('pcnf_pg_data', '') == '':
            # use default path
            self.config['pcnf_pg_data'] = '/var/lib/pgsql/data'

        if not os.path.exists(self.config.get('pcnf_pg_data', '')):
            raise GateException('Cannot find database component tablespace on disk')

    def _get_pg_config(self):
        """
        Get entire PostgreSQL configuration.
        """
        stdout, stderr = self.syscall("sudo", "-u", "postgres", "/bin/bash",
                                      input=self.get_scenario_template(target='psql').replace('@scenario', 'show all'))
        if stdout:
            for line in stdout.strip().split("\n")[2:]:
                try:
                    key, val = map(lambda line: line.strip(), line.split('|')[:2])
                    self.config['pcnf_' + key] = val
                except Exception:
                    print("Cannot parse line:", line)
        else:
            eprint(stderr)
            raise Exception("Underlying error: unable get backend configuration.")

    def _cleanup_pids(self):
        """
        Cleanup PostgreSQL garbage in /tmp
        """
        for fname in os.listdir('/tmp'):
            if fname.startswith('.s.PGSQL.'):
                os.unlink('/tmp/' + fname)

        # Remove postgresql.pid (versions 9.x) if postmaster was just killed
        if os.path.exists(self._pid_file):
            eprint('Info: Found stale PID file, removing')
            os.unlink(self._pid_file)

    @staticmethod
    def _get_conf(conf_path):
        """
        Get a PostgreSQL config file into a dictionary.
        """
        if not os.path.exists(conf_path):
            raise GateException('Cannot open config at "{0}".'.format(conf_path))

        conf = {}
        for line in open(conf_path).readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                key, val = [el.strip() for el in line.split('#')[0].strip().split('=', 1)]
                conf[key] = val
            except Exception:
                raise GateException("Cannot parse line '{0}' in '{1}'.".format(line, conf_path))

        return conf

    @staticmethod
    def _write_conf(conf_path, *table, **data):
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
                for items in data.items():
                    cfg.write('%s = %s\n' % items)
            elif table and not data:
                for items in table:
                    cfg.write('\t'.join(items) + "\n")
            cfg.close()
        else:
            raise IOError("Cannot write two different types of config into the same file!")

        return backup

    # Commands
    def do_db_start(self, **args):  # pylint: disable=W0613
        """
        Start the SUSE Manager Database.
        """
        print("Starting database...\t", end="")
        sys.stdout.flush()

        if self._get_db_status():
            print("failed")
            time.sleep(1)
            return

        # Cleanup first
        self._cleanup_pids()

        # Start the db
        cwd = os.getcwd()
        os.chdir(self.config.get('pcnf_data_directory', '/var/lib/pgsql'))
        if self._with_systemd:
            result = os.system('systemctl start postgresql.service')
        else:
            # TODO: This is obsolete code, going to be removed after 2.1 EOL
            result = os.system("sudo -u postgres /usr/bin/pg_ctl start -s -w -p /usr/bin/postmaster -D %s -o %s 2>&1>/dev/null"
                               % (self.config['pcnf_pg_data'], self.config.get('sysconfig_POSTGRES_OPTIONS', '""')))
        print(result and "failed" or "done")
        os.chdir(cwd)
        time.sleep(1)

    def do_db_stop(self, **args):  # pylint: disable=W0613
        """
        Stop the SUSE Manager Database.
        """
        print("Stopping database...\t", end="")
        sys.stdout.flush()

        if not self._get_db_status():
            print("failed")
            time.sleep(1)
            return

        # Stop the db
        if not self.config.get('pcnf_data_directory'):
            raise GateException("Cannot find data directory.")
        cwd = os.getcwd()
        os.chdir(self.config.get('pcnf_data_directory', '/var/lib/pgsql'))
        if self._with_systemd:
            result = os.system('systemctl stop postgresql.service')
        else:
            # TODO: This is obsolete code, going to be removed after 2.1 EOL
            result = os.system("sudo -u postgres /usr/bin/pg_ctl stop -s -D %s -m fast"
                               % self.config.get('pcnf_data_directory', ''))
        print(result and "failed" or "done")
        os.chdir(cwd)

        # Cleanup
        self._cleanup_pids()

    def do_db_status(self, **args):  # pylint: disable=W0613
        """
        Show database status.
        """
        print('Database is', self._get_db_status() and 'online' or 'offline')

    def do_space_tables(self, **args):  # pylint: disable=W0613
        """
        Show space report for each table.
        """
        stdout, stderr = self.call_scenario('pg-tablesizes', target='psql')

        if stderr:
            eprint(stderr)
            raise GateException("Unhandled underlying error occurred, see above.")

        if stdout:
            t_index = []
            t_ref = {}
            t_total = 0
            longest = 0
            for line in stdout.strip().split("\n")[2:]:
                line = list(filter(None, map(lambda el: el.strip(), line.split('|'))))
                if len(line) == 3:
                    t_name, t_size_pretty, t_size = line[0], line[1], int(line[2])
                    t_ref[t_name] = t_size_pretty
                    t_total += t_size
                    t_index.append(t_name)

                    longest = len(t_name) if len(t_name) > longest else longest

            t_index.sort()

            table = [('Table', 'Size',)]
            for name in t_index:
                table.append((name, t_ref[name],))
            table.append(('', '',))
            table.append(('Total', ('%.2f' % round(t_total / 1024. / 1024)) + 'M',))
            print("\n", TablePrint(table), "\n")

    @staticmethod
    def _get_partition(fdir):
        """
        Get partition of the directory.
        """
        return os.popen("df -lP %s | tail -1 | cut -d' ' -f 1" % fdir).read().strip()

    def do_space_overview(self, **args):  # pylint: disable=W0613
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
            """
            Info structure
            """
            fs_dev = None
            fs_type = None
            size = None
            used = None
            available = None
            used_prc = None
            mountpoint = None

        info = Info()
        for line in os.popen("df -T").readlines()[1:]:
            line = line.strip()
            if not line.startswith(partition):
                continue
            line = list(filter(None, line.split(" ")))
            info.fs_dev = line[0]
            info.fs_type = line[1]
            info.size = int(line[2]) * 1024  # Bytes
            info.used = int(line[3]) * 1024  # Bytes
            info.available = int(line[4]) * 1024  # Bytes
            info.used_prc = line[5]
            info.mountpoint = line[6]

            break

        # Get database sizes
        stdout, stderr = self.syscall("sudo", "-u", "postgres", "/bin/bash",
                                      input=self.get_scenario_template(target='psql').replace(
                                          '@scenario', 'select pg_database_size(datname), datname from pg_database;'))
        self.to_stderr(stderr)
        overview = [('Database', 'DB Size (Mb)', 'Avail (Mb)', 'Partition Disk Size (Mb)', 'Use %',)]
        for line in stdout.split("\n"):
            if "|" not in line or "pg_database_size" in line:  # Different versions of postgresql
                continue
            line = list(filter(None, line.strip().replace('|', '').split(" ")))
            if len(line) != 2:
                continue
            d_size = int(line[0])
            d_name = line[1]
            overview.append((d_name, self._bt_to_mb(d_size),
                             self._bt_to_mb(info.available),
                             self._bt_to_mb(info.size),
                             '%.3f' % round((float(d_size) / float(info.size) * 100), 3)))

        print("\n", TablePrint(overview), "\n")

    def do_space_reclaim(self, **args):  # pylint: disable=W0613
        """
        Free disk space from unused object in tables and indexes.
        """
        print("Examining database...\t", end="")
        sys.stdout.flush()

        if not self._get_db_status():
            time.sleep(1)
            raise GateException("Database must be online.")

        eprint("finished")
        time.sleep(1)

        operations = [
            ('Analyzing database', 'vacuum analyze;'),
            ('Reclaiming space', 'cluster;'),
            ]

        for msg, operation in operations:
            print("%s...\t" % msg, end="")
            sys.stdout.flush()

            _, stderr = self.syscall("sudo", "-u", "postgres", "/bin/bash",
                                     input=self.get_scenario_template(target='psql').replace('@scenario', operation))
            if stderr:
                eprint("failed")
                sys.stdout.flush()
                eprint(stderr)
                raise GateException("Unhandled underlying error occurred, see above.")

            print("done")
            sys.stdout.flush()

    @staticmethod
    def _get_tablespace_size(path):
        """
        Get tablespace size in bytes.
        """
        return int(os.popen('/usr/bin/du -bc %s' % path).readlines()[-1].strip().replace('\t', ' ').split(' ')[0])

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
        for fname in fpath:
            fname = path + "/" + fname
            if os.path.isdir(fname):
                found = self._rst_get_backup_root(fname)
                if found:
                    break

        return found

    def _rst_save_current_cluster(self):
        """
        Save current tablespace
        """
        old_data_dir = os.path.dirname(self.config['pcnf_pg_data']) + '/data.old'
        if not os.path.exists(old_data_dir):
            os.mkdir(old_data_dir)
            print("Created \"%s\" directory." % old_data_dir)

        print("Moving broken cluster:\t ", end="")
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
                eprint("Error: Unable to stop database.")
                sys.exit(1)

    def _rst_replace_new_backup(self, backup_dst):
        """
        Replace new backup.
        """
        # Archive into a tgz backup and place it near the cluster
        print("Restoring from backup:\t ", end="")
        sys.stdout.flush()

        # Remove cluster in general
        print("Remove broken cluster:\t ", end="")
        sys.stdout.flush()
        shutil.rmtree(self.config['pcnf_pg_data'])
        print("finished")
        sys.stdout.flush()

        # Unarchive cluster
        print("Unarchiving new backup:\t ", end="")
        sys.stdout.flush()
        roller = Roller()
        roller.start()

        destination_tar = backup_dst + "/base.tar.gz"
        temp_dir = tempfile.mkdtemp(dir=os.path.join(backup_dst, "tmp"))
        pguid = pwd.getpwnam('postgres')[2]
        pggid = grp.getgrnam('postgres')[2]
        os.chown(temp_dir, pguid, pggid)
        tar_command = '/bin/tar xf %s --directory=%s 2>/dev/null' % (destination_tar, temp_dir)
        os.system(tar_command)

        roller.stop("finished")
        time.sleep(1)

        print("Restore cluster:\t ", end="")
        backup_root = self._rst_get_backup_root(temp_dir)
        mv_command = '/bin/mv %s %s' % (backup_root, os.path.dirname(self.config['pcnf_pg_data']) + "/data")
        os.system(mv_command)

        print("finished")
        sys.stdout.flush()

        print("Write recovery.conf:\t ", end="")
        recovery_conf = os.path.join(self.config['pcnf_pg_data'], "recovery.conf")
        cfg = open(recovery_conf, 'w')
        cfg.write("restore_command = 'cp " + backup_dst + "/%f %p'\n")
        cfg.close()

        # Set recovery.conf correct ownership (SMDBA is running as root at this moment)
        data_owner = get_path_owner(self.config.get('pcnf_pg_data', PgBackup.DEFAULT_PG_DATA))
        os.chown(recovery_conf, data_owner.uid, data_owner.gid)

        print("finished")
        sys.stdout.flush()

    def do_backup_restore(self, *opts, **args):  # pylint: disable=W0613
        """
        Restore the SUSE Manager Database from backup.
        """
        # Go out from the current position, in case user is calling SMDBA inside the "data" directory
        location_begin = os.getcwd()
        os.chdir('/')

        # This is the ratio of compressing typical PostgreSQL cluster tablespace
        ratio = 0.134

        backup_dst, backup_on = self.do_backup_status('--silent')
        if not backup_on:
            eprint("No backup snapshots are available.")
            sys.exit(1)

        # Check if we have enough space to fit enough copy of the tablespace
        curr_ts_size = self._get_tablespace_size(self.config['pcnf_pg_data'])
        bckp_ts_size = self._get_tablespace_size(backup_dst)
        disk_size = self._get_partition_size(self.config['pcnf_pg_data'])

        print("Current cluster size:\t", self.size_pretty(curr_ts_size))
        print("Backup size:\t\t", self.size_pretty(bckp_ts_size))
        print("Current disk space:\t", self.size_pretty(disk_size))
        print("Predicted space:\t", self.size_pretty(disk_size - (curr_ts_size * ratio) - bckp_ts_size))

        # At least 1GB free disk space required *after* restore from the backup
        if disk_size - curr_ts_size - bckp_ts_size < 0x40000000:
            eprint("At least 1GB free disk space required after backup restoration.")
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

        # Move back where backup has been invoked
        os.chdir(location_begin)


    def do_backup_hot(self, *opts, **args):  # pylint: disable=W0613
        """
        Enable continuous archiving backup
        @help
        --enable=<value>\tEnable or disable hot backups. Values: on | off | purge
        --backup-dir=<path>\tDestination directory of the backup.\n
        """

        # Part for the auto-backups
        # --source\tSource path of WAL entry.\n
        # Example:
        # --autosource=%p --destination=/root/of/your/backups\n
        # NOTE: All parameters above are used automatically!\n

        if args.get('enable') == 'on' and 'backup-dir' in args.keys() and not args['backup-dir'].startswith('/'):
            raise GateException("No relative paths please.")

        # Already enabled?
        arch_cmd = list(filter(None, eval(self._get_conf(self.config['pcnf_pg_data'] +
                                                         "/postgresql.conf").get("archive_command", "''")).split(" ")))
        if '--destination' not in arch_cmd and args.get('enable') != 'on':
            raise GateException('Backups are not enabled. Please enable them first. See help for more information.')

        if '--destination' in arch_cmd:
            target = re.sub(r"/+$", "", eval(arch_cmd[arch_cmd.index("--destination") + 1].replace("%f", '')))
            if re.sub(r"/+$", "", args.get('backup-dir', target)) != target:
                raise GateException(("You've specified \"%s\" as a destination,\n" +
                                     "but your backup is already in \"%s\" directory.\n" +
                                     "In order to specify a new target directory,\n" +
                                     "you must purge (or disable) current backup.") % (args.get('backup-dir'), target))
            args['backup-dir'] = target
            if not args.get('enable'):
                args['enable'] = 'on'

        if args.get('enable') == 'on' and 'backup-dir' not in args.keys():
            raise GateException("Backup destination is not defined. Please issue '--backup-dir' option.")

        if 'enable' in args.keys():
            # Check destination only in case user is enabling the backup
            if args.get('enable') == 'on':
                # Same owner?
                if os.lstat(args['backup-dir']).st_uid != os.lstat(self.config['pcnf_pg_data']).st_uid \
                       or os.lstat(args['backup-dir']).st_gid != os.lstat(self.config['pcnf_pg_data']).st_gid:
                    raise GateException("The \"%s\" directory must belong to the "
                                        "same user and group as \"%s\" directory."
                                        % (args['backup-dir'], self.config['pcnf_pg_data']))
                # Same permissions?
                if oct(os.lstat(args['backup-dir']).st_mode & 0o777) != oct(os.lstat(self.config['pcnf_pg_data']).st_mode & 0o777):
                    raise GateException("The \"%s\" directory must have the same permissions as \"%s\" directory."
                                        % (args['backup-dir'], self.config['pcnf_pg_data']))
            self._perform_enable_backups(**args)

        if 'source' in args.keys():
            # Copy xlog entry
            self._perform_archive_operation(**args)

        print("INFO: Finished")

    def _perform_enable_backups(self, **args):
        """
        Turn backups on or off.
        """
        enable = args.get('enable', 'off')
        conf_path = self.config['pcnf_pg_data'] + "/postgresql.conf"
        conf = self._get_conf(conf_path)
        backup_dir = args.get('backup-dir')

        if enable == 'on':
            # Enable backups
            if not self._get_db_status():
                self.do_db_start()
            if not self._get_db_status():
                raise GateException("Cannot start the database!")

            if not os.path.exists(backup_dir):
                os.system('sudo -u postgres /bin/mkdir -p -m 0700 %s' % backup_dir)

            # first write the archive_command and restart the db
            # if we create the base backup after this, we prevent a race conditions
            # and do not lose archive logs
            cmd = "'" + "/usr/bin/smdba-pgarchive --source \"%p\" --destination \"" + backup_dir + "/%f\"'"
            if conf.get('archive_command', '') != cmd:
                conf['archive_command'] = cmd
                self._write_conf(conf_path, **conf)
                self._apply_db_conf()

            # round robin of base backups
            if os.path.exists(backup_dir + "/base.tar.gz"):
                if os.path.exists(backup_dir + "/base-old.tar.gz"):
                    os.remove(backup_dir + "/base-old.tar.gz")
                os.rename(backup_dir + "/base.tar.gz", backup_dir + "/base-old.tar.gz")

            b_dir_temp = os.path.join(backup_dir, 'tmp')
            cwd = os.getcwd()
            os.chdir(self.config.get('pcnf_data_directory', '/var/lib/pgsql'))
            os.system('sudo -u postgres /usr/bin/pg_basebackup -D {0}/ -Ft -c fast -X fetch -v -P -z'.format(b_dir_temp))
            os.chdir(cwd)

            if os.path.exists("{0}/base.tar.gz".format(b_dir_temp)):
                os.rename("{0}/base.tar.gz".format(b_dir_temp), "{0}/base.tar.gz".format(backup_dir))

            # Cleanup/rotate backup
            PgBackup(backup_dir, pg_data=self.config.get('pcnf_data_directory', '/var/lib/pgsql')).cleanup_backup()

        else:
            # Disable backups
            if enable == 'purge' and os.path.exists(backup_dir):
                print("INFO: Removing the whole backup tree \"%s\"" % backup_dir)
                shutil.rmtree(backup_dir)

            cmd = "'/bin/true'"
            if conf.get('archive_command', '') != cmd:
                conf['archive_command'] = cmd
                self._write_conf(conf_path, **conf)
                self._apply_db_conf()
            else:
                print("INFO: Backup was not enabled.")

    def _apply_db_conf(self):
        """
        Reload the configuration.
        """
        stdout, stderr = self.call_scenario('pg-reload-conf', target='psql')
        if stderr:
            eprint(stderr)
            raise GateException("Unhandled underlying error occurred, see above.")
        if stdout and stdout.strip() == 't':
            print("INFO: New configuration has been applied.")

    @staticmethod
    def _perform_archive_operation(**args):
        """
        Performs an archive operation.
        """
        if not args.get('source'):
            raise GateException("Source file was not specified!")

        if not os.path.exists(args.get('source')):
            raise GateException("File \"%s\" does not exists." % args.get('source'))

        if os.path.exists(args.get('backup-dir')):
            raise GateException("Destination file \"%s\"already exists." % args.get('backup-dir'))

        shutil.copy2(args.get('source'), args.get('backup-dir'))

    def do_backup_status(self, *opts, **args):  # pylint: disable=W0613
        """
        Show backup status.
        """
        backup_dst = ""
        backup_on = False
        conf_path = self.config['pcnf_pg_data'] + "/postgresql.conf"
        cmd = self._get_conf(conf_path).get('archive_command', '').split(" ")
        found_dest = False
        for comp in cmd:
            if comp.startswith('--destination'):
                found_dest = True
            elif found_dest:
                backup_dst = os.path.dirname(comp.replace('"', '').replace("'", ''))
                backup_on = os.path.exists(backup_dst)
                break

        backup_last_transaction = 0
        if backup_dst:
            for fname in os.listdir(backup_dst):
                mtime = os.path.getmtime(backup_dst + "/" + fname)
                if mtime > backup_last_transaction:
                    backup_last_transaction = mtime

        space_usage = None
        if backup_dst:
            partition = self._get_partition(backup_dst)
            for line in os.popen("df -T").readlines()[1:]:
                line = line.strip()
                if not line.startswith(partition):
                    continue
                space_usage = (list(filter(None, line.split(' ')))[5] + '').replace('%', '')

        if '--silent' not in opts:
            print("Backup status:\t\t", (backup_on and 'ON' or 'OFF'))
            print("Destination:\t\t", (backup_dst or '--'))
            print("Last transaction:\t", backup_last_transaction and time.ctime(backup_last_transaction) or '--')
            print("Space available:\t", space_usage and str((100 - int(space_usage))) + '%' or '--')

        return backup_dst, backup_on

    @staticmethod
    def _get_partition_size(path):
        """
        Get a size of the partition, where path belongs to.
        """
        return int((list(filter(None, (os.popen("df -TB1 %s" % path).readlines()[-1] + '').split(' ')))[4] + '').strip())

    def do_system_check(self, *args, **params):
        """
        Common backend healthcheck.
        @help
        autotuning\t\tperform initial autotuning of the database
    --max_connections=<num>\tdefine maximal number of database connections (default: 400)
        """
        # Check enough space
        # Check hot backup setup and clean it up automatically
        conf_path = self.config['pcnf_pg_data'] + "/postgresql.conf"
        conf = self._get_conf(conf_path)
        changed = False

        #
        # Setup postgresql.conf
        #

        # Built-in tuner
        conn_lowest = 270
        conn_default = 400
        max_conn = int(params.get('max_connections', conn_default))
        if max_conn < conn_lowest:
            print('INFO: max_connections should be at least {0}'.format(conn_lowest))
            max_conn = conn_lowest

        if 'autotuning' in args:
            for item, value in PgTune(max_conn).estimate().config.items():
                if not changed and str(conf.get(item, None)) != str(value):
                    changed = True
                conf[item] = value

        # WAL should be at least archive.
        if conf.get('wal_level', '') != 'archive':
            conf['wal_level'] = 'archive'
            changed = True

        # WAL senders at least 5
        if not conf.get('max_wal_senders') or int(conf.get('max_wal_senders')) < 5:
            conf['max_wal_senders'] = 5
            changed = True

        # WAL keep segments must be non-zero
        if conf.get('wal_keep_segments', '0') == '0':
            conf['wal_keep_segments'] = 64
            changed = True

        # Should run in archive mode
        if conf.get('archive_mode', 'off') != 'on':
            conf['archive_mode'] = 'on'
            changed = True

        # Stub
        if not conf.get('archive_command'):
            conf['archive_command'] = "'/bin/true'"
            changed = True

        # max_locks_per_transaction
        if not conf.get('max_locks_per_transaction') or int(conf.get('max_locks_per_transaction')) < 100:
            conf['max_locks_per_transaction'] = 100
            changed = True

        # [Spacewalk-devel] option standard_conforming_strings in Pg breaks our code and data.
        if conf.get('standard_conforming_strings', 'on') != "'off'":
            conf['standard_conforming_strings'] = "'off'"
            changed = True

        # bnc#775591
        if conf.get('bytea_output', '') != "'escape'":
            conf['bytea_output'] = "'escape'"
            changed = True

        # bsc#1022286 - too low value for statistic_target
        if int(conf.get('default_statistics_target', 100)) <= 10:
            del conf['default_statistics_target']
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
            pg_hba_conf.append(list(filter(None, line.replace("\t", " ").split(' '))))

        replication_cfg = ['local', 'replication', 'postgres', 'peer']

        if replication_cfg not in pg_hba_conf:
            pg_hba_conf.append(replication_cfg)
            hba_changed = True

        #
        # Commit the changes
        #
        if changed or hba_changed:
            print("INFO: Database configuration has been changed.")
            if changed:
                conf_bk = self._write_conf(conf_path, **conf)
                if conf_bk:
                    print("INFO: Wrote new general configuration. Backup as", conf_bk)

            # hba save
            if hba_changed:
                conf_bk = self._write_conf(pg_hba_cnf_path, *pg_hba_conf)
                if conf_bk:
                    print("INFO: Wrote new client auth configuration. Backup as", conf_bk)

            # Restart
            if self._get_db_status():
                self._apply_db_conf()
            else:
                print("INFO: Configuration has been changed, but your database is right now offline.")
            self.do_db_status()
        else:
            print("INFO: No changes required.")

        print("System check finished")

        return True

    def startup(self):
        """
        Hooks before the PostgreSQL gate operations starts.
        """
        # Do we have sudo permission?
        self.check_sudo('postgres')

    def finish(self):
        """
        Hooks after the PostgreSQL gate operations finished.
        """


def get_gate(config):
    """
    Get gate to the database engine.
    """
    return PgSQLGate(config)
