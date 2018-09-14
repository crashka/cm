# -*- coding: utf-8 -*-
"""
"""

import re
import json
import yaml
import datetime as dt

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
        """Initialize with path to YAML config file
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


##################
# util functions #
##################

def unixtime(tz = None):
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
