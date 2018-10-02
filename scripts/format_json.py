#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import sys
import json

if __name__ == '__main__' and __package__ is None:
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from cm.utils import prettyprint

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: %s <file>" % (sys.argv[0]))

    with open(sys.argv[1]) as f:
        data = json.load(f)

    prettyprint(data)
