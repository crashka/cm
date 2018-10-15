# -*- coding: utf-8 -*-
"""
"""

import regex as re
import json
import datetime as dt

import yaml

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

    def __init__(self, path):
        """
        :param path: path to YAML config file
        """
        self.path = path
        if Config.cfg_profiles.get(self.path) is None:
            Config.cfg_profiles[self.path] = {}

    def config(self, section, profile = None):
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
    def __init__(self, values, strmeth = None):
        """
        :param values: either list/set/tuple of names, or dict (for specified values)
        :param strmeth: name of a string method to apply to LOV values (e.g. 'lower')
        """
        if collecttype(values):
            if strmeth:
                self._mydict = {m: getattr(m, strmeth)() for m in values if strtype(m)}
            else:
                self._mydict = {m: m for m in values if strtype(m)}
        elif isinstance(values, dict):
            self._mydict = values
        else:
            self._mydict = {}

    def __getattr__(self, key):
        try:
            return self._mydict[key]
        except KeyError:
            raise AttributeError()

    def members(self):
        """
        :return: set of all member (attribute) names
        """
        return set(self._mydict.keys())

    def values(self):
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

def str2date(datestr, fmt = STD_DATE_FMT):
    """
    :param datestr: string
    :param fmt: [optional] defaults to Y-m-d
    :return: dt.date object
    """
    return dt.datetime.strptime(datestr, fmt).date()

def date2str(date, fmt = STD_DATE_FMT):
    """
    :param date: dt.date object
    :param fmt: [optional] defaults to Y-m-d
    :return: string
    """
    return date.strftime(fmt)

def str2time(timestr, fmt = STD_TIME_FMT):
    """
    :param timestr: string
    :param fmt: [optional] defaults to H:M:S
    :return: dt.time object
    """
    if len(timestr) == 5 and fmt == STD_TIME_FMT:
        fmt = STD_TIME_FMT2
    return dt.datetime.strptime(timestr, fmt).time()

def time2str(time, fmt = STD_TIME_FMT):
    """
    :param time: dt.time object
    :param fmt: [optional] defaults to H:M:S
    :return: string
    """
    return time.strftime(fmt)

def datetimetz(date, time, tz):
    """
    :param date: either string or dt.date
    :param time: either string or dt.time
    :param tz: tzinfo
    :return: dt.datetime (with tzinfo)
    """
    if strtype(date):
        date = str2date(date)
    if strtype(time):
        time = str2time(time)
    return dt.datetime.combine(date, time, tz)

def strtype(val):
    """
    :param val:
    :return: bool
    """
    return isinstance(val, str)

def collecttype(val):
    """
    :param val:
    :return: bool
    """
    return isinstance(val, (set, list, tuple))

def unixtime(tz = None):
    """
    :param tz: [optional] tzinfo
    :return: int
    """
    return int(dt.datetime.now(tz).strftime('%s'))

def truthy(val):
    if isinstance(val, str) and val.lower() in ['0', 'false', 'no', 'off']:
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

#########################
# Trace logging support #
#########################

# copied from https://gist.github.com/numberoverzero/f803ebf29a0677b6980a5a733a10ca71
# (this is done properly!)

import logging
_trace_installed = False

def install_trace_logger():
    global _trace_installed
    if _trace_installed:
        return
    level = logging.TRACE = logging.DEBUG - 5

    def log_logger(self, message, *args, **kwargs):
        if self.isEnabledFor(level):
            self._log(level, message, args, **kwargs)
    logging.getLoggerClass().trace = log_logger

    def log_root(msg, *args, **kwargs):
        logging.log(level, msg, *args, **kwargs)
    logging.addLevelName(level, "TRACE")
    logging.trace = log_root
    _trace_installed = True
