
import os
import json
from fcache.cache import FileCache


class Credentials:

    def __init__(self, config_file: str = 'snowflake_credentials.json',
                 conn_name: str = '') -> None:
        """Instantiates instances of the needed params to locate creds file.

        Args:
            config_file: Name of .json configuration file following the
            format of connection_credentials_SAMPLE.json.
            conn_name: Name of connection within json file to use - it will
            use first set of credentials in the file if no argument is passed.

        """
        self.cache = FileCache('snowmobile', flag='cs')
        self.config_file = config_file
        self.conn_name = conn_name
        self.path_to_config = self.cache.get(r'path_to_config')

    def clear_cache(self) -> object:
        """Clears cached path to credentials file."""
        self.cache.clear()
        return self

    def cache_exists(self) -> bool:
        """Checks to see if a cached file path exists to a valid file."""
        try:
            return os.path.isfile(self.path_to_config)
        except:
            return False

    def cache_valid_for_config(self) -> bool:
        """Checks to see if the valid file path contains the config file."""
        try:
            return self.config_file == os.path.basename(self.path_to_config)
        except:
            return False

    def locate_config(self):
        """Find config file by traversing file system from bottom up."""
        for dirpath, dirnames, files in os.walk(os.path.expanduser('~'),
                                                topdown=False):
            if self.config_file in files:

                self.path_to_config = os.path.join(dirpath, self.config_file)
                break

            else:
                self.path_to_config = ''

        return self.path_to_config

    def get_creds_path(self) -> str:
        """Checks for cache existence and validates - traverses OS if not."""
        print("Locating credentials...")

        print("\t<1 of 2> Checking for cached path...")
        if self.cache_exists() and self.cache_valid_for_config():

            print(f"\t<2 of 2> Found cached path: {self.path_to_config}")

        else:

            print("\t<2 of 2> Cached path not found")
            print(f"\nLooking for {self.config_file} in local file system..")

            self.path_to_config = \
                self.locate_config()

            if self.path_to_config:
                print(f"\t<1 of 1> '{self.config_file}' found at: "
                      f"{self.path_to_config}")
            else:
                print(f"\t<1 of 1> Could not find config file"
                      f" {self.config_file} please double check the name of "
                      f"your configuration file or value passed in the"
                      f"'config_file' argument")

        return self.path_to_config

    @staticmethod
    def get_conn(all_creds: dict, conn_name: str, config_file: str):
        """Finds default connection in config file."

        Args:
            all_creds: Dictionary of all credentials
            conn_name: Name of connection (if passed)
            config_file: Name of config file (for console output only)

        Returns:
            Name of the set of credentials to be parsed out of the
            credentials file

        """
        print("\nImporting credentials...")
        if not conn_name:
            conn_name = next(iter(all_creds.keys()))
            print(f"\t<1 of 2> No explicit connection passed, fetching "
                  f"'{conn_name}' credentials by default")

        else:
            print(f"\t<1 of 2> Fetching user-specific 'conn_name={conn_name}' "
                  f"from {config_file}")

        return conn_name

    def get(self) -> dict:
        """Locates creds file and parses out the specified set of credentials.

        Returns:
            Dictionary containing a specific set of Snowflake credentials

        """
        self.path_to_config = self.get_creds_path()
        self.cache['path_to_config'] = self.path_to_config

        try:
            with open(self.path_to_config) as c:
                all_creds = json.load(c)
                all_creds = {k.lower: v for k, v in all_creds.items()}

        except IOError as e:
            print(e)

        try:
            self.conn_name = self.get_conn(all_creds, self.conn_name,
                                           self.config_file)
            creds = all_creds.get(self.conn_name.lower())
            print(f"\t<2 of 2> Successfully imported credentials for "
                  f"'{self.conn_name}'")
            return creds

        except IOError as e:
            print(
                f"\t<2 of 2>Could not parse conn_name='{self.conn_name}' "
                f"from {self.config_file}\nPlease either "
                f"\n\t\t- Specify a different configuration file or "
                f"connection name"
                f"\n\t\t- Verify the contents of the configuration file"
                f"\n\t\t- Run snowmobile.snowcreds.cache.clear()")
            print(e)
