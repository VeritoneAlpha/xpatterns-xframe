#
#   Environment
#
#   Provides a structure for maintaining an application environment.
#   The environment is read from a set of configuration files.

import ConfigParser
import os
import logging


class ConfigError(Exception):
    pass


def get_xframes_home():
    import xframes
    return os.path.dirname(xframes.__file__)


def get_xframes_config():
    if 'XFRAMES_CONFIG_DIR' in os.environ:
        return os.environ['XFRAMES_CONFIG_DIR']
    return None


class Environment(object):
    def __init__(self):
        """ Create an empty environment. """
        self._files_to_read = None
        self._files_read = None
        self.cfg = ConfigParser.SafeConfigParser()

    @staticmethod
    def create(config_files=None):
        """
        Create an Environment by reading default config files.

        Parameters
        ----------
        config_files : [ string ]
            A list of config file names in ini file format.

        Returns
        -------
        Environment
            The environment resulting from the ini file(s).

        """
        files_to_read = [os.path.join(get_xframes_home(), 'default.ini')]
        if config_files:
            files_to_read.append(config_files)
        config_dir = get_xframes_config()
        if config_dir:
            files_to_read.append(os.path.join(config_dir, 'config.ini'))
        env = Environment()
        # We depend on the order of files read, so that config.ini overrides default.ini.
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
            logging.warn("FAILED -- missing section or option: {} {}".format(section, item))
            logging.warn(e)
            return None
        except ConfigError as e:
            logging.warn("FAILED -- missing config file")
            logging.warn(e)
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
        except (ConfigError, ConfigParser.NoSectionError):
            return {}
