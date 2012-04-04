import time
import sys
import threading


#
# Infinite progress bar for console.
#
# Author: Bo Maryniuk <bo@suse.de>
#

class Roller(threading.Thread):
    """
    Roller of some fun sequences while waiting.
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self.__sequence = ['-', '\\', '|', '/',]
        self.__freq = .1
        self.__offset = 0
        self.__running = False
        self.__message = None


    def run(self):
        self.__running = True
        while self.__running:
            if self.__offset > len(self.__sequence) - 1:
                self.__offset = 0

            sys.stdout.write("\b" + self.__sequence[self.__offset])
            sys.stdout.flush()
            time.sleep(self.__freq)

            self.__offset += 1

        print >> sys.stdout, "\b" + self.__message
        sys.stdout.flush()


    def stop(self, message=None):
        self.__message = message and message or "  "
        self.__running = False
        self.__offset = 0



if __name__ == '__main__':
    print >> sys.stdout, "Doing thing:\t",
    sys.stdout.flush()

    roller = Roller()
    roller.start()
    time.sleep(5)
    roller.stop("finished")
    time.sleep(1)
    print >> sys.stdout, "OK"
