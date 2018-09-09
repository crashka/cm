# -*- coding: utf-8 -*-
"""
"""

import re
import json

##################
# util functions #
##################

def truthy(val):
    if isinstance(val, basestring) and val.lower() in ['0', 'false', 'no']:
        return False
    else:
        return bool(val)

def prettyprint(data, indent=4, noprint=False):
    """Nicer version of pprint (which is actually kind of ugly)

    Note: assumes that input data can be dumped to json (typically a list or dict)
    """
    pattern = re.compile(r'^', re.MULTILINE)
    spaces = ' ' * indent
    if noprint:
        return re.sub(pattern, spaces, json.dumps(data, indent=indent, sort_keys=True))
    else:
        print(re.sub(pattern, spaces, json.dumps(data, indent=indent, sort_keys=True)))
