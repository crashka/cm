# -*- coding: utf-8 -*-

"""'Data Science' module
"""

import sys
import hashlib

# this assumes a signed value for maxsize
NBITS  = sys.maxsize.bit_length() + 1
NBYTES = NBITS // 4

def strhash(s):
    """
    :param s: input string
    :return: int
    """
    s = s.encode('utf-8')
    # convert unsigned hex to signed int (ignorant of NBITS derivation)
    return int(int(hashlib.sha1(s).hexdigest()[-NBYTES:], 16) - ((1 << (NBITS - 1)) - 1))

class HashSeq(object):
    def __init__(self, depth = 3):
        self.depth = depth
        self.values = []

    def add(self, s, force = False):
        """Skip duplicate hash (return None), unless force specified
        """
        curhash = strhash(s)
        if self.values and curhash == self.values[-1] and not force:
            return None
        self.values.append(0)
        for i in range(len(self.values)):
            self.values[i] ^= curhash
        return self.get()

    def get(self):
        # length of the return list represents the "level" of element [0]
        return self.values[-self.depth:]
