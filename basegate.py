# Base gate 
# Author: Bo Maryniuk <bo@suse.de>
#


class BaseGate:
    """
    Gate of tools for all supported databases.
    """

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
