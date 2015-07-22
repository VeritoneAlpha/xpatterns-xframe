#
#   Environment
#
#   Provides a structure for maintaining an application environment.
#   The environment is read from a set of configuration files.

import ConfigParser
import os
from sys import stderr


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
        files_to_read = ['default.ini', config_file]
        env.read(files_to_read)
        return env

    @staticmethod
    def create_default():
        config_dir = os.environ['XFRAMES_HOME'] if 'XFRAMES_HOME' in os.environ else '.'
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

    def get_config_items(self, section):
        """
        Gets all option values from a named section of the environment.

        Parameters
        ----------
        section : string
            The section name.

        Returns
        -------
        out: dict
            The option values.

        """
        try:
            items = self.cfg.items(section, raw=True)
            return items
        except ConfigParser.NoSectionError as e:
            print >>stderr, "FAILED -- missing section: ", section
            print >>stderr, e
            return {}
        except ConfigError as e:
            print >>stderr, "FAILED -- missing config file"
            print >>stderr, e
            return {}
