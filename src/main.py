import configparser

def read_config(conf_file: str) -> dict:
    """
    Reads a .conf file into a dictionary.
    Supports simple KEY = VALUE format (quotes optional).
    """
    config = {}
    parser = configparser.RawConfigParser()

    with open(conf_file, "r") as f:
        for line in f:
            if "=" in line:
                key, val = line.strip().split("=", 1)
                config[key.strip()] = val.strip().strip('"').strip("'")
    return config
