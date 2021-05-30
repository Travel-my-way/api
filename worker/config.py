"""
Configuration file

This file holds all the required config values pulled from external sources likes API keys.
It is fully compatible with python-dotenv :)
"""
import os

OUIBUS_API_KEY = os.getenv("OUIBUS_API_KEY")
NAVITIA_API_KEY = os.getenv("NAVITIA_API_KEY")
ORS_API_KEY = os.getenv("ORS_API_KEY")
SKYSCANNER_API_KEY = os.getenv("SKYSCANNER_API_KEY")
SKYSCANNER_RAPIDAPI_KEY = os.getenv("SKYSCANNER_RAPIDAPI_KEY")
KOMBO_API_KEY = os.getenv("KOMBO_API_KEY")
BLABLACAR_API_KEY = os.getenv("BLABLACAR_API_KEY")
