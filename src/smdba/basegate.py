# coding: utf-8
"""
Base gate class for specific databases.
"""

import os
import sys
import textwrap
import abc
import typing
from subprocess import Popen, PIPE, STDOUT
from smdba.utils import eprint

# pylint: disable=W0622


class GateException(Exception):
    """
    Gate exception.
    """


class BaseGate(metaclass=abc.ABCMeta):
    """
    Gate of tools for all supported databases.
    """

    debug = False

    def __init__(self):
        self.config = {}
        self._gate_commands = {}

    @staticmethod
    def is_sm_running() -> bool:
        """
        Performs a pretty basic check without checking all the components.

        :returns True in case SUSE Manager is running.
        """
        initd = '/etc/init.d'
        print("Checking SUSE Manager running...")

        # Get tomcat
        tomcat = ""
        for cmd in os.listdir(initd):
            if cmd.startswith('tomcat'):
                tomcat = initd + "/" + cmd
                break

        return os.popen(tomcat + " status 2>&1").read().strip().find('dead') == -1

    @staticmethod
    def get_scn(name):
        """
        Get scenario by name.
        """
        scenario = os.path.sep.join((os.path.abspath(__file__).split(os.path.sep)[:-1] + ['scenarios', name + ".scn"]))
        if not os.path.exists(scenario):
            raise IOError("Scenario '{}' is not accessible.".format(scenario))

        return open(scenario, 'r')

    def get_scenario_template(self, target='sqlplus', login=None):
        """
        Generate a template for the Oracle SQL*Plus scenario.
        """
        env = os.environ.get
        scenario = []
        login = login if login else '/nolog'

        if target == 'sqlplus':
            executable = "/bin/%s -S %s" % (target, login)
        elif target == 'rman':
            executable = "/bin/" + target
        elif target == 'psql':
            executable = "/usr/bin/" + target
        else:
            raise Exception("Unknown scenario target: %s" % target)

        if target in ['sqlplus', 'rman']:
            if env('PATH') and env('ORACLE_BASE') and env('ORACLE_SID') and env('ORACLE_HOME'):
                scenario.append("export ORACLE_BASE=" + env('ORACLE_BASE'))
                scenario.append("export ORACLE_SID=" + env('ORACLE_SID'))
                scenario.append("export ORACLE_HOME=" + env('ORACLE_HOME'))
                scenario.append("export PATH=" + env('PATH'))
            else:
                raise Exception("Underlying error: environment cannot be constructed.")

            scenario.append("cat - << EOF | " + env('ORACLE_HOME') + executable)
            if target == 'sqlplus' and login.lower() == '/nolog':
                scenario.append("CONNECT / AS SYSDBA;")
            elif target == 'rman':
                scenario.append("CONNECT TARGET /")

            scenario.append("@scenario")
            scenario.append("EXIT;")
            scenario.append("EOF")
        elif target in ['psql']:
            scenario.append(("cat - << EOF | " + executable + " -t --pset footer=off " + self.config.get('db_name', '')).strip())
            scenario.append("@scenario")
            scenario.append("EOF")

        if self.debug:
            print("\n" + ("-" * 40) + "8<" + ("-" * 40))
            print('\n'.join(scenario))
            print(("-" * 40) + "8<" + ("-" * 40))

        return '\n'.join(scenario)

    def call_scenario(self, scenario, target='sqlplus', login=None, **variables):
        """
        Call scenario in SQL*Plus.
        Returns stdout and stderr.
        """
        template = self.get_scenario_template(target=target, login=login).replace(
            '@scenario', self.get_scn(scenario).read().replace('$', r'\$'))
        if variables:
            for k_var, v_var in variables.items():
                template = template.replace('@' + k_var, v_var)

        if target in ['sqlplus', 'rman']:
            user = 'oracle'
        elif target in ['psql']:
            user = 'postgres'
        else:
            raise GateException("Unknown target: %s" % target)
        return self.syscall("sudo", template, None, "-u", user, "/bin/bash")
    @staticmethod
    def to_bytes(value: str) -> bytes:
        """
        Convert string to bytes.

        :param value: string
        :returs: bytes
        """
        if value is not None:
            value = value.encode("utf-8")
        return value

    @staticmethod
    def to_str(value: bytes) -> str:
        """
        Convert bytes to str.

        :param value: bytes
        :returns: string
        """
        if value is not None:
            value = value.decode("utf-8")
        return value

    def syscall(self, command, *params, input=None) -> typing.Tuple[str, str]:
        """
        Call an external system command.

        :param command: a command to run from a system.
        :param input: input device.
        :param *params: tertiary parameters
        :returns: STDOUT/STDERR tuple
        """
        stdout, stderr = Popen([command] + list(params), stdout=PIPE, stdin=PIPE, stderr=STDOUT,
                               env=os.environ).communicate(input=input)
        if not stderr:
            stderr = ""
        stderr += self.extract_errors(stdout)

        return stdout and stdout.strip() or '', stderr and stderr.strip() or ''

    def get_gate_commands(self) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Gate commands inspector.

        :returns: dictionary of the gate commands.
        """
        gate_commands = getattr(self, "_gate_commands", None)
        if not gate_commands:
            self._gate_commands = {}

        for method_name in dir(self):
            if not method_name.startswith("do_"):
                continue

            help = {}
            descr = [line.strip() for line in getattr(self, method_name).__doc__.strip().split("\n")]
            help['description'] = descr[0]
            if len(descr) > 1:
                cutoff = True
                helptext = []
                for line in descr:
                    if line == '@help':
                        cutoff = False
                        continue
                    if not cutoff:
                        helptext.append(line)
                help['help'] = '\n'.join(helptext)
            self._gate_commands[method_name] = help

        return self._gate_commands

    @abc.abstractmethod
    def check(self):
        """
        Check for the gate requirements.
        """
    @staticmethod
    def size_pretty(size: str, int_only: bool = False, no_whitespace: bool = False) -> str:
        """
        Make pretty size from bytes to other metrics.
        Size: amount (int, long)
        """

        _size = float(size)
        wsp = "" if no_whitespace else " "
        wrap = lambda dummy: dummy if not int_only else int
        sz_ptn = '%s' if int_only else '%.2f'

        if _size >= 0x10000000000:
            msg = (sz_ptn + '%sTB') % (wrap((_size / 0x10000000000)), wsp)
        elif _size >= 0x40000000:
            msg = (sz_ptn + '%sGB') % (wrap((_size / 0x40000000)), wsp)
        elif _size >= 0x100000:
            msg = (sz_ptn + '%sMB') % (wrap((_size / 0x100000)), wsp)
        elif _size >= 0x400:
            msg = (sz_ptn + '%sKB') % (wrap((_size / 0x400)), wsp)
        else:
            msg = ((int_only and '%s' or '%.f') + '%sBytes') % (wrap(_size), wsp)
        return msg

    @staticmethod
    def media_usage(path: str) -> typing.Dict[str, float]:
        """
        Return media usage statistics about the given path.

        Returned valus is a dictionary with keys 'total', 'used' and
        'free', which are the amount of total, used and free space, in bytes.
        """
        stvf = os.statvfs(path)
        free = stvf.f_bavail * stvf.f_frsize
        total = stvf.f_blocks * stvf.f_frsize
        used = (stvf.f_blocks - stvf.f_bfree) * stvf.f_frsize

        return {'free': free, 'total': total, 'used': used}

    def check_sudo(self, uid) -> None:
        """
        Check if UID has sudo permission.

        :raises GateException if access denied.
        :returns: None
        """
        stdout, stderr = self.syscall(os.popen("which sudo").read().strip(), "", None,
                                      "-nu", uid, "-S", "true", "/bin/bash")
        if stdout or stderr:
            raise GateException("Access denied to UID '{}' via sudo.".format(uid))

    @abc.abstractmethod
    def startup(self):
        """
        Gate-specific hooks before starting any operations.
        """

    @abc.abstractmethod
    def finish(self):
        """
        Gate-specific hooks after finishing all operations.
        """

    @staticmethod
    def extract_errors(stdout):
        """
        Extract errors from the RMAN and SQLPlus.
        Based on http://docs.oracle.com/cd/B28359_01/backup.111/b28270/rcmtroub.htm
        List of the errors: http://docs.oracle.com/cd/B28359_01/server.111/b28278/toc.htm
        """
        if not (stdout + "").strip():
            return ""

        out = []
        for line in filter(None, str(stdout).replace("\\n", "\n").split("\n")):
            if line.lower().startswith("ora-") or line.lower().startswith("rman-"):
                if not line.find("===") > -1:
                    out += textwrap.wrap(line.strip())

        return '\n'.join(out)

    @staticmethod
    def to_stderr(stderr):
        """
        Format an error output to STDERR and terminate everything at once.
        """
        if not (stderr + "").strip():
            return False

        out = []
        for line in filter(None, str(stderr).replace("\\n", "\n").split("\n")):
            out.append("  " + line.strip())

        eprint("\nError:\n" + ("-" * 80))
        eprint("\n".join(out))
        eprint("-" * 80)

        sys.exit(1)
