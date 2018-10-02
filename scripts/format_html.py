#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import sys
from bs4 import BeautifulSoup

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: %s <file>" % (sys.argv[0]))

    with open(sys.argv[1]) as f:
        soup = BeautifulSoup(f, 'lxml')

    print(soup.prettify().encode('utf-8'))
