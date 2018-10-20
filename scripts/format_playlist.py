#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
from glob import glob
import os.path

from bs4 import BeautifulSoup

if __name__ == '__main__' and __package__ is None:
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from cm.core import BASE_DIR
from cm.utils import prettyprint

DFLT_HTML_PARSER = 'html.parser'

if __name__ == '__main__':
    if len(sys.argv) not in (3, 4):
        raise RuntimeError("Usage: %s <station> <playlist> [<html_parser>]" % (sys.argv[0]))

    station = sys.argv[1].upper()
    playlist = sys.argv[2]
    html_parser = sys.argv[3] if len(sys.argv) > 3 else DFLT_HTML_PARSER

    pattern = os.path.join(BASE_DIR, 'stations', station, 'playlists', playlist) + '*'
    pl_files = glob(pattern)
    for file in pl_files:
        type = os.path.splitext(file)[1]
        if type == '.json':
            with open(file) as f:
                data = json.load(f)
            prettyprint(data)
        elif type == '.html':
            with open(file) as f:
                soup = BeautifulSoup(f, html_parser)
            print(soup.prettify())
