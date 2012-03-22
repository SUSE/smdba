# Base gate 
# Author: Bo Maryniuk <bo@suse.de>
#

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
        return Popen([command] + list(params), 
                     stdout=PIPE, 
                     stdin=PIPE, 
                     stderr=STDOUT).communicate(input=input)


    def get_gate_commands(self):
        """
        Gate commands inspector.
        """

        gate_commands = getattr(self, "_gate_commands", None)
        if not gate_commands:
            self.gate_commands = {}

        for method_name in dir(self):
            if not method_name.startswith("do_"):
                continue

            self.gate_commands[method_name] = getattr(self, method_name).__doc__.strip()

        return self.gate_commands


if __name__ == "__main__":
    b = BaseGate()
    print b.syscall("echo", None, None, "hello")
