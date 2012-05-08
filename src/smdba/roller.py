# Visuals :)
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



# Test
if __name__ == '__main__':
    print >> sys.stdout, "Doing thing:\t",
    sys.stdout.flush()

    roller = Roller()
    roller.start()
    time.sleep(5)
    roller.stop("finished")
    time.sleep(1)
    print >> sys.stdout, "OK"
