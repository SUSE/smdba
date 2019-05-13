# coding: utf-8
"""
Visual console "toys".
"""

import time
import sys
import threading
import typing


class Roller(threading.Thread):
    """
    Roller of some fun sequences while waiting.
    """

    def __init__(self) -> None:
        threading.Thread.__init__(self)
        self.__sequence = ['-', '\\', '|', '/',]
        self.__freq = .1
        self.__offset = 0
        self.__running = False
        self.__message: typing.Optional[str] = None

    def run(self) -> None:
        """
        Run roller.

        :return: None
        """
        self.__running = True
        while self.__running:
            if self.__offset > len(self.__sequence) - 1:
                self.__offset = 0

            sys.stdout.write("\b" + self.__sequence[self.__offset])
            sys.stdout.flush()
            time.sleep(self.__freq)

            self.__offset += 1

        print("\b" + (self.__message or ""))
        sys.stdout.flush()

    def stop(self, message: typing.Optional[str] = None) -> None:
        """
        Stop roller.

        :param message: Message for the roller.
        :return: None
        """
        self.__message = message if message else "  "
        self.__running = False
        self.__offset = 0


# if __name__ == '__main__':
#     print("Doing thing:\t", end="")
#     sys.stdout.flush()
#
#     roller = Roller()
#     roller.start()
#     time.sleep(5)
#     roller.stop("finished")
#     time.sleep(1)
#     print("OK")
