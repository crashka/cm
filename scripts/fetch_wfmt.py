#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Special script for downloading WFMT playlists using SeleniumBase.  LATER, integrate as
an alternate download mechanism (configurable by station)!!!
"""

from numbers import Number
from datetime import datetime, timedelta
import random
import json
import time
import sys

from seleniumbase import Driver

from cm.utils import str2date, date2str

def typecast(val: str) -> str | Number | bool:
    """Simple logic for casting a string value to the appropriate type; usable for
    parameters coming from a program command line or HTML form
    """
    if val.isdecimal():
        return int(val)
    if val.isnumeric():
        return float(val)
    if val.lower() in ['false', 'f', 'no', 'n']:
        return False
    if val.lower() in ['true', 't', 'yes', 'y']:
        return True
    if val.lower() in ['null', 'none', 'nil']:
        return None
    return val if len(val) > 0 else None

def parse_argv(argv: list[str]) -> tuple[list, dict]:
    """Takes a list of arguments (typically a slice of sys.argv), which may be a
    combination of bare agruments or kwargs-style constructions (e.g. "key=value") and
    returns a tuple of ``args`` and ``kwargs``.  For both ``args`` and ``kwargs``, we
    attempt to cast the value to the proper type (e.g. int, float, bool, or None).
    """
    args = []
    kwargs = {}
    args_done = False
    for arg in argv:
        if not args_done:
            if '=' not in arg:
                args.append(typecast(arg))
                continue
            else:
                args_done = True
        kw, val = arg.split('=', 1)
        kwargs[kw] = typecast(val)

    return args, kwargs

########
# main #
########

SLEEP_MIN = 1.0
STATUS_OK = "ok"
TS_FMT    = '%Y%m%d_%H%M%S'

def sleep_rnd() -> None:
    """Sleep for a random amount of time, greater than ``SLEEP_MIN``.
    """
    time.sleep(SLEEP_MIN + random.random() / 2.0)

def main() -> int:
    """Fetch playlists for WFMT (hardwired URL for now).

    Usage::

      python -m fetch_wfmt [start=<start_date>] [end=<end_date>] [verbose=<bool>]
    """
    verbose = False
    start = None
    end = None

    args, kwargs = parse_argv(sys.argv[1:])
    if len(args) > 0:
        args_str = ' '.join(str(arg) for arg in args)
        raise RuntimeError("Unexpected argument(s): " + args_str)
    start = str2date(kwargs.pop('start'))
    if not start:
        raise RuntimeError("Start date must be specified")
    end = str2date(kwargs.pop('end'))
    if not end or end < start:
        raise RuntimeError("Bad end date specified")
    verbose = kwargs.pop('verbose', False)

    driver = Driver(uc=True, headless=True)

    playlists_info = {}
    ts = datetime.now().strftime(TS_FMT)
    playlists_file = f"WFMT_playlists-{ts}.json"
    day = start
    oneday = timedelta(days=1)
    while day <= end:
        pldate = date2str(day, '%m/%d/%Y')
        url = f'https://www.wfmt.com/schedule/?pldate={pldate}'
        datestr = date2str(day)
        filename = f'{datestr}.html'
        if verbose:
            print(f"Fetching url: {url}...", end='', flush=True)
        driver.get(url)
        with open(filename, 'w') as f:
            nbytes = f.write(driver.page_source)
        if verbose:
            print(f"{nbytes} bytes written")
        playlists_info[datestr] = {
            "file":   filename,
            "size":   nbytes,
            "status": STATUS_OK
        }
        sleep_rnd()
        day += oneday

    driver.quit()
    with open(playlists_file, 'w') as f:
        json.dump(playlists_info, f, indent=2)
    return 0

if __name__ == '__main__':
    sys.exit(main())
