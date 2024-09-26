"""
Configuration module for loading application settings.

This module reads the settings from a JSON file and initializes
a Settings object with the loaded data.
"""

import json
import os

from computes.settings.settings_model import Settings

# Get the directory path of the current file
dir_path = os.path.dirname(os.path.realpath(__file__))

# Open and read the settings JSON file
settings_file = open(os.path.join(dir_path, 'settings.json'), 'r')
settings_dict = json.load(settings_file)

# Initialize the Settings object with the loaded data
settings: Settings = Settings(**settings_dict)

# Note: Remember to close the file after reading
settings_file.close()
