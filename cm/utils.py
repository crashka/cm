# -*- coding: utf-8 -*-
"""
"""

from __future__ import absolute_import, division, print_function

import re
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
    def __init__(self, values, strfunc = None):
        """
        :param values: either list/set/tuple of names, or dict (for specified values)
        :param strfunc: name of a string method to apply to LOV values (e.g. 'lower')
        """
        if type(values) in (list, set, tuple):
            if strfunc:
                self._mydict = {m: getattr(m, strfunc)() for m in values if strtype(m)}
            else:
                self._mydict = {m: m for m in values if strtype(m)}
        elif type(values) == dict:
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

STD_DATE_FMT   = '%Y-%m-%d'  # same as ISO 8601

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

def strtype(string):
    """
    :param string:
    :return: bool
    """
    return type(string) in (unicode, str)

def unixtime(tz = None):
    """
    :param tz: [optional] tzinfo
    :return: int
    """
    return int(dt.datetime.now(tz).strftime('%s'))

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
