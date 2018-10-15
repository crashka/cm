#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from bs4 import BeautifulSoup

DFLT_HTML_PARSER = 'html.parser'

if __name__ == '__main__':
    if len(sys.argv) not in (2, 3):
        raise RuntimeError("Usage: %s <file> [<parser>]" % (sys.argv[0]))

    with open(sys.argv[1]) as f:
        soup = BeautifulSoup(f, sys.argv[2] if len(sys.argv) > 2 else DFLT_HTML_PARSER)

    print(soup.prettify())
