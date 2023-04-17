import yaml
from pathlib import Path


class ConfigLoader:
    def __init__(self, base_path, config_file):
        self.base_path = Path(base_path)
        self.config_file = self.base_path / config_file
        self.config = self.load_config()

    def load_config(self, path=None):
        if path:
            config_path = path
        else:
            config_path = self.config_file
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def get_config(self):
        return self.config
    
    def get_source_keys(self):
        return list(self.config['sources'].keys())
    
    def get_config_path(self, *args):
        path = Path(self.base_path)
        for arg in args:
            if arg:
                path = path / arg
        return str(path)
    
    def config_generator(self):
        keys = self.get_source_keys()
        for key in keys:
            credenstial_path = self.get_config_path(key, 'credenstials.yml')
            credenstial = self.load_config(credenstial_path)
            tables = self.get_config()['sources'][key]
            for table in tables:
                table_path = self.get_config_path(key, 'tables', table)
                table_info = self.load_config(table_path)
                yield key, credenstial, table_info