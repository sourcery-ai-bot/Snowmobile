import os
import json


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

        self.config_file = config_file
        self.conn_name = conn_name

    def get(self) -> dict:
        """Locates creds file and parses out the specified set of credentials.

        Returns:
            Dictionary containing a specific set of Snowflake credentials

        """
        print("Locating & importing credentials..")
        print(f"\t<1 of 4> Searching for {self.config_file} in local file "
              f"system..")
        for dirpath, dirnames, files in os.walk(os.path.expanduser('~'),
                                                topdown=False):
            if self.config_file in files:
                path_to_config = os.path.join(dirpath, self.config_file)
                break
            else:
                path_to_config = ''
                pass

        if not os.path.isfile(path_to_config):
            print(f"\tCould not find {self.config_file} in file system!")
        else:
            with open(path_to_config) as c:
                all_creds = json.load(c)
            print(f"\n\t<2 of 4> Located & loaded {self.config_file} "
                  f"from:\n\t\t{path_to_config}")

        if not self.conn_name:
            self.conn_name = next(iter(all_creds.keys()))
            print(f"\n\t<3 of 4> No explicit connection passed, fetching "
                  f"'{self.conn_name}' credentials by default")
        else:
            print(f"\n\t<3 of 4> Fetching '{self.conn_name}' credentials "
                  f"from {self.config_file}")
            # pass

        creds = all_creds.get(self.conn_name)

        if creds:
            print(f"\n\t<4 of 4> Successfully imported credentials for "
                  f"conn_name='{self.conn_name}'")

        else:
            print(f"\n\t<4 of 4> Could not parse conn_name='{self.conn_name}'"
                  f"from {self.config_file}\n\t\tPlease specify a different "
                  f"configuration file, connection name, or check that the "
                  f"contents of the configuration file")

        return creds

# creds = Credentials().get()
# creds.get()
# creds.conn_name
