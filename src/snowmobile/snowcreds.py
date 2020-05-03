
# Imports
import os
import json


class Credentials:

    def __init__(self, config_file: str = 'snowflake_config.json',
                 conn_name: str = '') -> None:
        """

        Args:
            config_file: Name of .json configuration file following the
            format of connection_credentials_SAMPLE.json.
            conn_name: Name of connection within json file to use - it will
            use first set of credentials in the file if no argument is passed.
        """
        self.config_file = config_file
        self.conn_name = conn_name

    def get(self):
        print(f"Searching for {self.config_file} in local file system..\n")
        for dirpath, dirnames, files in os.walk(os.path.expanduser('~'),
                                                topdown=False):
            if self.config_file in files:
                path_to_config = os.path.join(dirpath, self.config_file)
                break
            else:
                path_to_config = ''
                pass

        if not path_to_config:
            print(f"Could not find {self.config_file} in file system")
        else:
            with open(path_to_config) as c:
                all_creds = json.load(c)
            print(f"Located & loaded {self.config_file} from:\n\t"
                  f"{path_to_config}")

        if not self.conn_name:
            self.conn_name = next(iter(all_creds.keys()))
        else:
            pass

        creds = all_creds.get(self.conn_name)

        return creds


# creds = Credentials().get()
# creds.get()
# creds.conn_name

