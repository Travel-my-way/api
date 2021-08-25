from typing import Tuple
from pathlib import Path


def get_points(from_loc: str, to_loc: str) -> Tuple[list, list]:
    """Returns parameters in a more usable way"""

    start = from_loc.split(",")
    start[0] = float(start[0])
    start[1] = float(start[1])
    arrival = to_loc.split(",")
    arrival[0] = float(arrival[0])
    arrival[1] = float(arrival[1])

    return start, arrival


def get_relative_file_path(base_path: str, filename: str) -> Path:
    """Returns a filepath relative to this one"""
    return Path(base_path).parent.absolute() / filename
