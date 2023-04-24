from src.Config import ConfigLoader
from src.MySql_connector import PyMySQL
from src.Riot_api import RiotAPI, DataDragonAPI


def run_pipeline(generator):
    while True:
        try:
            key, credentials, tables_info = next(generator)
            if key == 'sql':
                # handle sql case
                pass
            elif key == 'api':
                # handle api case
                pass
            elif key == 'file':
                # handle file case
                pass
        except StopIteration:
            print('End of pipeline')
            break
        except FileNotFoundError as e:
            print(f"File Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    config = ConfigLoader('configs/', 'job_config.yml')
    generator = config.config_generator()
    run_pipeline(generator)