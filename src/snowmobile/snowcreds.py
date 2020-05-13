import os
import json
from fcache.cache import FileCache

cache = FileCache('snowmobile', flag='cs')


class Credentials:

    def __init__(self, config_file: str = 'snowflake_credentials.json',
                 conn_name: str = '', cache=cache) -> None:
        """Instantiates instances of the needed params to locate creds file.

        Args:
            config_file: Name of .json configuration file following the
            format of connection_credentials_SAMPLE.json.
            conn_name: Name of connection within json file to use - it will
            use first set of credentials in the file if no argument is passed.

        """
        self.cache = cache
        self.config_file = config_file
        self.conn_name = conn_name.lower()
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

    def get(self) -> dict:
        """Locates creds file and parses out the specified set of credentials.

        Returns:
            Dictionary containing a specific set of Snowflake credentials

        """
        self.path_to_config = self.get_creds_path()
        self.cache['path_to_config'] = self.path_to_config

        try:
            with open(self.path_to_config) as c:
                temp = json.load(c)
                # print(temp)
                # self.all_creds = temp
                self.all_creds = \
                    {k.lower(): v for k, v in temp.items()}

        except IOError as e:
            print(e)

        print("\nImporting credentials...")
        if not self.conn_name:
            self.conn_name = next(iter(self.all_creds.keys()))
            print(f"\t<1 of 2> No explicit connection passed, fetching "
                  f"'{self.conn_name}' credentials by default")

        else:
            print(f"\t<1 of 2> Fetching user-specific 'conn_name="
                  f"'{self.conn_name}' from {self.config_file}")
            self.conn_name = self.conn_name


        # try:
        # self.conn_name = get_conn(self.all_creds, self.conn_name,
        #                           self.config_file)
        self.creds = self.all_creds[self.conn_name]

        if self.creds and self.conn_name:
            print(f"\t<2 of 2> Successfully imported credentials for "
                  f"'{self.conn_name}'")


        # except IOError as e:
        else:
            print(
                f"\t<2 of 2> Could not parse conn_name='{self.conn_name}' "
                f"from {self.config_file}\n\nPlease either \n\t\t- Specify "
                f"a different configuration file or connection name "
                f"\n\t\t- Verify the contents of the configuration file"
                f"\n\t\t- Run snowmobile.snowcreds.cache.clear()")
            # print(e)

        return self.creds
