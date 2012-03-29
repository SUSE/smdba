# Base gate 
# Author: Bo Maryniuk <bo@suse.de>
#

import os
import subprocess
from subprocess import Popen, PIPE, STDOUT


class BaseGate:
    """
    Gate of tools for all supported databases.
    """

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

            self._gate_commands[method_name] = getattr(self, method_name).__doc__.strip()

        return self._gate_commands
