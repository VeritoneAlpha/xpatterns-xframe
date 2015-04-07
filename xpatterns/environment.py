#
#   Environment
#
#   Provides a structure for maintaining an application environment.
#   The environment is read from a set of configuration files.

import ConfigParser
import os
from sys import stderr
from operator import itemgetter
from itertools import chain

class ConfigError(Exception):
    pass

class Environment(object):
    def __init__(self):
        """ Create an empty environment. """
        self.files_to_read = []
        self.files_read = []
        defaults = {}
        for v in os.environ:
            if v in v in os.environ:
                defaults[v] = os.environ[v]
        self.cfg = ConfigParser.SafeConfigParser(defaults)

    @staticmethod
    def create_empty():
        """ Create and return an empty environment. """
        return Environment()

    @staticmethod
    def create_from_file(config_file):
        """
        Create an Environment by reading an ini file and default config files.

        Parameters
        ----------
        config_file : string
            The file name of a file in ini file format.

        Returns
        -------
        Environment
            The environment resulting from the ini file(s).

        """
        env = Environment()
        files_to_read = []

        # defaults
        files_to_read.append('default.ini')
        # general-purpose config files, such as for resource files
        files_to_read.append(config_file)
        env.read(files_to_read)
        return env

    @staticmethod
    def create_default():
        config_dir = os.environ['XPATTERNS_HOME'] if 'XPATTERNS_HOME' in os.environ else '.'
        return Environment.create_from_file(os.path.join(config_dir, 'config.ini'))

    def read(self, files_to_read):
        files_read = self.cfg.read(files_to_read)
        self.files_to_read.extend(files_to_read)
        self.files_read.extend(files_read)

    def get_config(self, section, item, default=None):
        """
        Gets option value from a named section of the environment.

        Parameters
        ----------
        section : string
            The section name.
        item : string
            The option name.
        default: string, optional
            The default value

        Returns
        -------
        string
            The option value.

        """
        try:
            res = self.cfg.get(section, item, False)
            return res
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            if default is not None:
                return default
            print >>stderr, "FAILED -- missing section or option: ", section, item
            print >>stderr, e
            return None
        except ConfigError as e:
            print >>stderr, "FAILED -- missing config file"
            print >>stderr, e
            return None

    def get_config_list(self, section, prefix, default=None):
        """
        Gets list of option values from a named section of the environment.

        Parameters
        ----------
        section : string
            The section name.
        option : string
            The option base name.  The values returned are formed using two methods.

            1. all option names starting with `option` are collected.
            2. all the values obtained in this way are split using ';'.

        Returns
        -------
        [str]
            The option value.
        """
        try:
            items = self.cfg.items(section, False, Environment.default_config_vars())
            matches = [i for i in items if i[0].startswith(prefix)]
            vals = [i[1].split(';') for i in sorted(matches, key=itemgetter(0))]
            return [x for x in list(chain(*vals))]
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            if default:
                return [default]
            print >>stderr, "FAILED -- missing section in config file:", section
            print >>stderr, e
            return None
        except ConfigParser.InterpolationMissingOptionError as e:
            print >>stderr, "FAILED -- missing substitution in config file DEFAULT section"
            print >>stderr, e
            return None
        except ConfigError as e:
            print >>stderr, "FAILED -- missing config file"
            print >>stderr, e
            return None

