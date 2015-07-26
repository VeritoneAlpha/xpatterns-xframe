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

def get_xframes_home():
    import xframes
    return os.path.dirname(xframes.__file__)

class Environment(object):
    def __init__(self):
        """ Create an empty environment. """
        self._files_to_read = None
        self._files_read = None
        self.cfg = ConfigParser.SafeConfigParser()

    @staticmethod
    def create(config_files=None):
        """
        Create an Environment by reading an ini file and default config files.

        Parameters
        ----------
        config_files : [ string ]
            A list of config file names in ini file format.

        Returns
        -------
        Environment
            The environment resulting from the ini file(s).

        """
        files_to_read = []
        files_to_read.append(os.path.join(get_xframes_home(), 'xframes/default.ini'))
        files_to_read.append('config.ini')
        if config_files: files_to_read.append(config_files)
        env = Environment()
        env._read(files_to_read)
        return env

    def _read(self, files_to_read):
        self._files_to_read = files_to_read
        self._files_read = self.cfg.read(files_to_read)

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
            return {item[0]: item[1] for item in items}
        except (ConfigError, ConfigParser.NoSectionError) as e:
            return {}
