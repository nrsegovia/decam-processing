#!/usr/bin/env python3

import logging
from parquet_utils import *
from utils import *
from constants import *
from pathlib import Path
import argparse
import re

'''
Main script. Call other functions and constants from here
'''

local = Path(__file__).parent
valid_fields = list(ALL_FIELDS.keys())
valid_modes = list(MODES.keys())
valid_bands = "griz"

# ARGUMENT PARSING FUNCTIONS

def parse_ccds(value):
    """
    Parse a single CCD or range in format 'start-end'.
    Returns either an int (for single number) or a 
    list including all integers in the specified range.
    """
    # Check if it's a range (contains exactly one dash between numbers)
    if '-' in value:
        # Use regex to match the pattern: number-number
        match = re.match(r'^(\d+)-(\d+)$', value)
        if not match:
            raise argparse.ArgumentTypeError(f"Invalid range format: '{value}'. Expected format: 'start-end' (e.g., '1-10')")
        
        start, end = int(match.group(1)), int(match.group(2))
        
        if end <= start:
            raise argparse.ArgumentTypeError(f"Invalid range: '{value}'. End number ({end}) must be greater than start number ({start})")
        
        if in_ccd_range(start) & in_ccd_range(end):
            return list(range(start, end + 1)), False
        else:
            raise argparse.ArgumentTypeError(f"Invalid range: '{value}'. Edges must be within [1-61].")
    else:
        # Single number
        if in_ccd_range(value):
            try:
                return int(value), True
            except ValueError:
                raise argparse.ArgumentTypeError(f"Invalid number: '{value}'. Expected a single number or range (e.g., '1-10')")
        else:
            raise argparse.ArgumentTypeError(f"Invalid number: '{value}'. Number must be within [1-61].")

def parse_bands(value):
    """
    Parse a single or multiple bands. Returns either a string.
    """
    # Check if it's a list based on length
    is_list = len(value) > 1
    if is_list:
        check_list = any([x not in valid_bands for x in value])
        if not check_list:
            raise argparse.ArgumentTypeError(f"Invalid band list format: '{value}'. Expected a string made of the following characters: {valid_bands}, e.g., 'gri', 'iz', 'g'.")
        return value, False
    else:
        # Single band
        if value in valid_bands:
            return int(value), True
        else:
            raise argparse.ArgumentTypeError(f"Invalid band: '{value}'. Expected one of {valid_bands}.")

def parse_directory(value):
    if value in valid_fields:
        return value
    else:
        raise argparse.ArgumentTypeError(f"Invalid directory: '{value}'. Expected one of {valid_fields}.")
    
def parse_mode(value):
    if value in valid_modes:
        return value
    else:
        raise argparse.ArgumentTypeError(f"Invalid mode: '{value}'. Expected one of {valid_modes}.")


# MODE INITIALIZATION FUNCTIONS

def hdf_to_parquet_mode(main_dir: Path, ccds, single_ccd, bands, single_band, logger):
    
    logger.info(f"Converting HDF files to Parquet...")
    directories = []

    # Single CCD and single band
    if single_ccd and single_band:
        directories.append(Path(main_dir, ccds, bands))
    # Single CCD and multiple bands
    elif single_ccd:
        for band in bands:
            directories.append(Path(main_dir, ccds, band))
    # Single band and multiple CCDs
    elif single_band:
        for ccd in ccds:
            directories.append(Path(main_dir, ccd, bands))
    # Multiple CCDs and multiple bands
    else:
        for ccd in ccds:
            for band in bands:
                directories.append(Path(main_dir, ccd, band))

    # Call the relevant function for all necessary directories:
    for target_dir in directories:
        success, fail, errors = HDF5_directory_to_parquet(target_dir, logger, 4)
        logger.info(f"Finished {target_dir} with {success} succesfully verified, {fail} non-verified, and {errors} error file(s).")

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(
        description="Tools for processing photpipe-reduced DECam photometry",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Add arguments with --name value format
    parser.add_argument(
        '--directory', 
        type=parse_directory, 
        required=True,
        help=f'Name of the directory as defined in the constants.py file: {valid_fields}.'
    )
    
    parser.add_argument(
        '--bands', 
        type=str, 
        default="g,r,i,z",
        help='Bands to work with, comma separated without spaced. The default is "g,r,i,z", all available bands.'
    )
    
    parser.add_argument(
        '--ccds', 
        type=parse_ccds,
        required=True,
        help='CCDs to work with. Can be a single digit or a range provided as e.g., 1-20.'
    )

    parser.add_argument(
        '--mode', 
        type=parse_mode,
        help=f'Mode to use. Can be one of the following: \n{MODES_STRING}'
    )
    
    # Add flags (boolean arguments)
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='Enable verbose output'
    )
    
    # Parse the arguments
    args = parser.parse_args()
    ccds, single_ccd = args.ccds
    bands, single_band = args.bands
    main_dir = ALL_FIELDS[args.directory]
    mode = args.mode

    # Initialize logging
    logging.basicConfig(filename=Path(local, "processing.log"),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    if mode == "HDF_TO_PARQUET":
        hdf_to_parquet_mode(main_dir, ccds, single_ccd, bands, single_band, logger)

if __name__ == "__main__":
    main()