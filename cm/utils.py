# -*- coding: utf-8 -*-
"""
"""

from typing import Any
from collections.abc import Mapping, Iterable
import regex as re
import json
import datetime as dt

import yaml
import Levenshtein

#####################
# config management #
#####################

class Config(object):
    """Manages YAML config information, features include:
      - Caching by config file
      - Fetching by section
      - Overlays on 'default' profile
    """
    cfg_profiles = dict()  # {config_file: {profile_name: {section_name: ...}}}

    def __init__(self, path: str):
        """
        :param path: path to YAML config file
        """
        self.path = path
        if Config.cfg_profiles.get(self.path) is None:
            Config.cfg_profiles[self.path] = {}

    def config(self, section: str, profile: str = None) -> dict:
        """Get config section for specified profile

        :param section: section within profile (or 'default')
        :param profile: [optional] if specified, overlay entries on top of 'default' profile
        :return: dict indexed by key
        """
        if profile in Config.cfg_profiles[self.path]:
            return Config.cfg_profiles[self.path][profile].get(section, {})

        with open(self.path, 'r') as f:
            cfg = yaml.safe_load(f)
        if cfg:
            prof_data = cfg.get('default', {})
            if profile:
                prof_data.update(cfg.get(profile, {}))
            Config.cfg_profiles[self.path][profile] = prof_data
        else:
            Config.cfg_profiles[self.path][profile] = {}

        return Config.cfg_profiles[self.path][profile].get(section, {})


################
# util classes #
################

class LOV(object):
    """Create a closed List Of Values based on a dict or list/set/tuple of LOV names.  If
    only names are provided, corresponding values will be generated based on the names
    (either a straight copy, or with the specified string method applied)
    """
    def __init__(self, values: Mapping | Iterable, strmeth: str = None):
        """
        :param values: either list/set/tuple of names, or dict (for specified values)
        :param strmeth: name of a string method to apply to LOV values (e.g. 'lower')
        """
        if isinstance(values, Mapping):
            self._mydict = values
        elif isinstance(values, Iterable):
            assert not isinstance(values, str)
            if strmeth:
                self._mydict = {m: getattr(m, strmeth)() for m in values if isinstance(m, str)}
            else:
                self._mydict = {m: m for m in values if isinstance(m, str)}
        else:
            # REVISIT: shouldn't really ignore `values`!!!
            self._mydict = {}

    def __getattr__(self, key):
        try:
            return self._mydict[key]
        except KeyError:
            raise AttributeError()

    def members(self) -> set:
        """
        :return: set of all member (attribute) names
        """
        return set(self._mydict.keys())

    def values(self) -> set:
        """
        :return: set of all values
        """
        return set(self._mydict.values())


##################
# util functions #
##################

# same as ISO 8601
STD_DATE_FMT  = '%Y-%m-%d'
STD_TIME_FMT  = '%H:%M:%S'
STD_TIME_FMT2 = '%H:%M'

def str2date(datestr: str, fmt: str = STD_DATE_FMT) -> dt.date:
    """
    :param datestr: string
    :param fmt: [optional] defaults to Y-m-d
    :return: dt.date object
    """
    return dt.datetime.strptime(datestr, fmt).date()

def date2str(date: dt.date, fmt: str = STD_DATE_FMT) -> str:
    """
    :param date: dt.date object
    :param fmt: [optional] defaults to Y-m-d
    :return: string
    """
    return date.strftime(fmt)

def str2time(timestr: str, fmt: str = STD_TIME_FMT) -> dt.time:
    """
    :param timestr: string
    :param fmt: [optional] defaults to H:M:S
    :return: dt.time object
    """
    if len(timestr) == 5 and fmt == STD_TIME_FMT:
        fmt = STD_TIME_FMT2
    return dt.datetime.strptime(timestr, fmt).time()

def time2str(time: dt.time, fmt: str = STD_TIME_FMT) -> str:
    """
    :param time: dt.time object
    :param fmt: [optional] defaults to H:M:S
    :return: string
    """
    return time.strftime(fmt)

def str2dur(durstr: str, delim: str = ':', decimal: str = '.') -> dt.timedelta | None:
    """Parse duration string, assuming format of [hours:]minutes:seconds[.fractional], `None`
    is returned if input string is malformed

    Note that fractional part is truncated to microseconds
    """
    hours = 0
    mins  = 0
    secs  = 0
    usecs = 0

    dur_segs = durstr.split(delim)
    match len(dur_segs):
        case 2:
            mins = int(dur_segs[0])
        case 3:
            hours = int(dur_segs[0])
            mins = int(dur_segs[1])
        case _:
            return None

    sec_segs = dur_segs[-1].split(decimal)
    match len(sec_segs):
        case 1:
            secs = int(sec_segs[0])
        case 2:
            secs = int(sec_segs[0])
            usecs = int(sec_segs[1][:6].ljust(6, '0'))  # truncate or pad out to 6 digits
        case _:
            return None

    return dt.timedelta(hours=hours, minutes=mins, seconds=secs, microseconds=usecs)

def datetimetz(date: dt.date | str, time: dt.time | str, tz: dt.tzinfo) -> dt.datetime:
    """
    :param date: either string or dt.date
    :param time: either string or dt.time
    :param tz: pytz tzinfo
    :return: dt.datetime (with tzinfo)
    """
    if isinstance(date, str):
        date = str2date(date)
    if isinstance(time, str):
        time = str2time(time)
    return dt.datetime.combine(date, time, tz)

def unixtime(tz: dt.tzinfo = None) -> int:
    """
    :param tz: [optional] tzinfo
    :return: int
    """
    return int(dt.datetime.now(tz).strftime('%s'))

FALSEHOOD = {'0', 'false', 'f', 'no', 'n', 'off'}

def truthy(val: Any) -> bool:
    if isinstance(val, str) and val.lower() in FALSEHOOD:
        return False
    else:
        return bool(val)

def str_similarity(a: str, b: str) -> float:
    """
    :return: float (ratio) in the range [0, 1]
    """
    return Levenshtein.ratio(a, b)

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

#########################
# Trace logging support #
#########################

# from https://stackoverflow.com/questions/2183233/

import logging

TRACE = logging.DEBUG - 5
NOTICE = logging.WARNING + 5

class MyLogger(logging.getLoggerClass()):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)

        logging.addLevelName(TRACE, "TRACE")
        logging.addLevelName(NOTICE, "NOTICE")

    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(TRACE):
            self._log(TRACE, msg, args, **kwargs)

    def notice(self, msg, *args, **kwargs):
        """Currently just write to default logging channel, later perhaps write to separate
        channel, or store in database
        """
        if self.isEnabledFor(NOTICE):
            #kwargs['stack_info'] = True
            self._log(NOTICE, msg, args, **kwargs)

logging.setLoggerClass(MyLogger)
