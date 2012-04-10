# Base gate 
# Author: Bo Maryniuk <bo@suse.de>
#

import os
import subprocess
from subprocess import Popen, PIPE, STDOUT


class GateException(Exception): pass

class BaseGate:
    """
    Gate of tools for all supported databases.
    """


    def get_scenario_template(self, target='sqlplus'):
        """
        Generate a template for the Oracle SQL*Plus scenario.
        """        
        e = os.environ.get
        scenario = []

        executable = None
        if target == 'sqlplus':
            executable = "/bin/sqlplus -S /nolog"
        elif target == 'rman':
            executable = "/bin/rman"
        else:
            raise Exception("Unknown scenario target: %s" % target)

        if target in ['sqlplus', 'rman']:
            if e('PATH') and e('ORACLE_BASE') and e('ORACLE_SID') and e('ORACLE_HOME'):
                scenario.append("export ORACLE_BASE=" + e('ORACLE_BASE'))
                scenario.append("export ORACLE_SID=" + e('ORACLE_SID'))
                scenario.append("export ORACLE_HOME=" + e('ORACLE_HOME'))
                scenario.append("export PATH=" + e('PATH'))
            else:
                raise Exception("Underlying error: environment cannot be constructed.")

        scenario.append("cat - << EOF | " + e('ORACLE_HOME') + executable)
        if target == 'sqlplus':
            scenario.append("CONNECT / AS SYSDBA;")
        elif target == 'rman':
            scenario.append("CONNECT TARGET /")

        scenario.append("@scenario")
        scenario.append("EXIT;")
        scenario.append("EOF")


        return '\n'.join(scenario)

    
    def call_scenario(self, scenario, target='sqlplus', **variables):
        """
        Call scenario in SQL*Plus.
        Returns stdout and stderr.
        """
        if not os.path.exists(scenario):
            raise Exception("Underlying error: Scenario {scenario} does not exists or is unreachable.".format(scenario=scenario))
        template = self.get_scenario_template(target=target).replace('@scenario', open(scenario).read().replace('$', '\$'))

        if variables:
            for k_var, v_var in variables.items():
                template = template.replace('@' + k_var, v_var)

        return self.syscall("sudo", template, None, "-u", "oracle", "/bin/bash")


    def syscall(self, command, input=None, daemon=None, *params):
        """
        Call an external system command.
        """
        stdout, stderr = Popen([command] + list(params), 
                               stdout=PIPE, 
                               stdin=PIPE, 
                               stderr=STDOUT,
                               env=os.environ).communicate(input=input)

        return stdout and stdout.strip() or '', stderr and stderr.strip() or ''


    def get_gate_commands(self):
        """
        Gate commands inspector.
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
