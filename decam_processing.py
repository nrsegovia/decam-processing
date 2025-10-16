#!/usr/bin/env python3

import logging
from parquet_utils import *
from photpipe_utils import *
from utils import *
from constants import *
from pathlib import Path
import argparse
import re
import os
from matcher import *

'''
Main script. Call other functions and constants from here
'''

local = Path(__file__).parent
valid_fields = list(ALL_FIELDS.keys())
valid_global = list(GLOBAL_NAME_ONLY.keys())
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
        try:
            integer = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid input: '{value}'. Expected numeric.")
        if in_ccd_range(integer):
            try:
                return integer, True
            except ValueError:
                raise argparse.ArgumentTypeError(f"Invalid number: '{integer}'. Expected a single number or range (e.g., '1-10')")
        else:
            raise argparse.ArgumentTypeError(f"Invalid number: '{integer}'. Number must be within [1-61].")

def parse_bands(value):
    """
    Parse a single or multiple bands. Returns string.
    """
    # Check if it's a list based on length
    is_list = len(value) > 1
    if is_list:
        check_list = any([x not in valid_bands for x in value])
        if check_list:
            raise argparse.ArgumentTypeError(f"Invalid band list format: '{value}'. Expected a string made of the following characters: {valid_bands}, e.g., 'gri', 'iz', 'g'.")
        return value, False
    else:
        # Single band
        if value in valid_bands:
            return value, True
        else:
            raise argparse.ArgumentTypeError(f"Invalid band: '{value}'. Expected one of {valid_bands}.")

def parse_directory(value):
    if value in valid_fields:
        return value, True
    elif value in valid_global:
        return value, False
    else:
        raise argparse.ArgumentTypeError(f"Invalid directory: '{value}'. Expected one of {valid_fields} or {valid_global}.")
    
def parse_mode(value):
    if value in valid_modes:
        return value
    else:
        raise argparse.ArgumentTypeError(f"Invalid mode: '{value}'. Expected one of {valid_modes}.")
    
def parse_input(value):
    if Path(value).exists():
        return value
    else:
        raise argparse.ArgumentTypeError(f"Invalid input: '{value}'. Expected path to existing file.")
    
def parse_output(value):
    if Path(value).is_dir():
        return value
    else:
        raise argparse.ArgumentTypeError(f"Invalid input: '{value}'. Expected path to directory.")

def parse_radius(value):
    try:
        output = float(value)
        if (output <= 60.0) & (output > 0.0):
            return output
        else:
            raise Exception("Value out of bounds [0-60.0]")
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Invalid input: '{value}' due to {e}. Expected radius (<60) in arcseconds.")

# MODE INITIALIZATION FUNCTIONS

def hdf_to_parquet_mode(main_dir: Path, ccds, single_ccd, bands, single_band, workers, logger):
    
    logger.info(f"Converting HDF files to Parquet...")
    directories = list_directories(main_dir, ccds, single_ccd, bands, single_band)

    # Call the relevant function for all necessary directories:
    for target_dir in directories:
        if target_dir.exists():
            success, fail, errors, messages = HDF5_directory_to_parquet(target_dir, logger, workers)
            logger.info(f"Finished {target_dir} with {success} succesfully verified, {fail} non-verified, and {errors} error file(s).")
            if errors > 0:
                logger.info(f"Listing errors below.")
                for msg in messages:
                    if msg != "":
                        logger.error(msg)
            
            # Update the Header.info file
            try:
                replace_string_in_file(Path(target_dir, "Header.info"), ".hdf", ".parquet")
            except Exception:
                logger.error(f"Header.info file in {target_dir} failed to convert. Check the directory.")
        else:
            logger.info(f"{target_dir} does not exist. Skipping.")


def dcmp_to_parquet_mode(main_dir: Path, ccds, single_ccd, bands, single_band, workers, logger):
    
    logger.info(f"Converting dcmp files (photpipe catalogs) to Parquet...")
    directories, str_ccd = list_directories_no_band(main_dir, ccds, single_ccd)

    # Call the relevant function for all necessary directories:
    for index, target_dir in enumerate(directories):
        if target_dir.exists():
            ok, fail, total, messages = process_photpipe_dir(target_dir, str_ccd[index], bands, logger, workers)
            logger.info(f'CCD {str_ccd[index]} had {total} dcmp files.\n{ok} succeeded, {fail} failed. If any failed, find details below.')
            if fail > 0:
                logger.info(f"Listing errors below.")
                for msg in messages:
                    if msg != "":
                        logger.error(msg)
        else:
            logger.info(f"{target_dir} does not exist. Skipping.")

def create_db_ccd_band_mode(main_dir, ccds, single_ccd, bands, single_band, workers, glob_name, logger):
    """Run batch processing on given ccd-band(s) configuration.
    Creates/populates DB and master catalogue per band"""
    out_dir = Path(local, "output", glob_name)
    if single_ccd:
        these_ccds = [ccds]
    else:
        these_ccds = ccds
    # Process each subdirectory
    for number_ccd in these_ccds:
        current_ccd = str(number_ccd)
        logger.info(f"Working on CCD {current_ccd}.")
        create_db_ccd_band(logger, bands, main_dir, current_ccd, out_dir)

    logger.info("Batch processing completed!")

def master_catalog_ccd_mode(main_dir, ccds, single_ccd, bands, single_band, workers, glob_name, logger):
    logger.info(f"Started creation of final master catalog(s) for CCD(s): {ccds}")
    out_dir = Path(local, "output", glob_name)
    out_dir.mkdir(parents=True, exist_ok=True)
    if single_ccd:
        create_ccd_master_catalog(logger, glob_name, main_dir, ccds, out_dir)
        logger.info(f"CCD {ccds} done.")
    else:
        for ccd in ccds:
            create_ccd_master_catalog(logger, glob_name, main_dir, ccd, out_dir)
            logger.info(f"CCD {ccd} done.")
    logger.info(f"Final master catalog for CCD process has finished.")

def lightcurve_mode(main_dir, input_catalogue, input_ra, input_dec, input_radius, save_dir, glob_name, logger):
    logger.info(f"Started lightcurve extraction for input catalog: {input_catalogue}")
    save_dir.mkdir(parents=True, exist_ok=True)
    out_dir = Path(local, "output", glob_name)
    try:
        input_cat = pd.read_csv(input_catalogue)
    except Exception as e:
        logger.error(f"Problem found when loading file: {e}. Aborting.")
    found_ccds = [] # Tuple storing CCD number and masked rows.
    # CCD list must be created here using the json
    json_file = Path(out_dir, '..', "fields_info.json")
    if json_file.exists():
        with open(json_file, 'r') as f:
            json_data = json.load(f)
        current_field = json_data[glob_name]
        for stored_ccd in current_field.keys():
            this_ra_min = current_field[stored_ccd]["RA"][0]
            this_ra_max = current_field[stored_ccd]["RA"][1]
            this_dec_min = current_field[stored_ccd]["Dec"][0]
            this_dec_max = current_field[stored_ccd]["Dec"][1]
            mask = check_points_in_rectangle(input_cat,input_ra,input_dec,this_ra_min, this_ra_max, this_dec_min, this_dec_max)
            if any(mask):
                found_ccds.append([stored_ccd, mask])
    else:
        logger.error("JSON file defining fields not found. Aborting.")
    if len(found_ccds) > 0:
        for current_ccd, current_mask in found_ccds:
            df_subset = input_cat[current_mask]
            extract_light_curves(logger, glob_name, main_dir, current_ccd, out_dir, df_subset, input_ra, input_dec, input_radius, save_dir)
            logger.info(f"CCD {current_ccd} done.")
        logger.info(f"Lightcurve creation process has finished.")
    else:
        logger.info(f"No input catalogue sources were found in the data.")

def remove_duplicates_mode(main_dir, ccds, single_ccd, bands, single_band, workers, glob_name, logger):
    logger.info("Started removal of duplicates for all CCDs")
    for ccd in range(1,62):
        for band in "griz":
            relevant_paths = [Path(x, str(ccd), band) for x in main_dir]
            all_paths = []
            for this_path in relevant_paths:
                all_paths += list(this_path.glob("*.parquet"))
            check_and_handle_mjd_duplicates(logger, all_paths, main_dir)
        logger.info(f"CCD {ccd} done.")
    logger.info(f"Duplicates have been removed.")

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
        help=f'Name of the directory or global set name as defined in the constants.py file: {valid_fields} or {valid_global}.'
    )
    
    parser.add_argument(
        '--bands', 
        type=parse_bands, 
        default="griz",
        help='Bands to work with (together in a string). The default is "griz", all available bands.'
    )
    
    parser.add_argument(
        '--workers', 
        type=int, 
        default=min(32, (os.cpu_count() or 1) + 4),
        help='Number of workers to use when multithreading is involved. Defaults to min(32, (os.cpu_count() or 1) + 4)'
    )

    parser.add_argument(
        '--inputcat', 
        type=parse_input, 
        default=None,
        help='Input catalog to use for lightcurve extraction'
    )

    parser.add_argument(
        '--outdir', 
        type=parse_output, 
        default=Path(Path(__file__).parent, "output", "lightcurves"),
        help='Output directory to use for lightcurve extraction'
    )

    parser.add_argument(
        '--ra', 
        type=str, 
        default="RA",
        help='RA column (degrees) from input catalog'
    )

    parser.add_argument(
        '--dec', 
        type=str, 
        default="Dec",
        help='Dec column (degrees) from input catalog'
    )

    parser.add_argument(
        '--radius', 
        type=parse_radius, 
        default=1.0,
        help='Croossmatch radius for lightcurve extraction.'
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
        required=True,
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
    main_dir, path_only = args.directory
    if path_only:
        main_dir = Path(ALL_FIELDS[main_dir])
        glob_name = None
    else:
        glob_name = main_dir
        main_dir = GLOBAL_NAME_ONLY[main_dir]
    mode = args.mode
    input_path = args.inputcat
    output_dir = args.outdir
    input_ra = args.ra
    input_dec = args.dec
    input_radius = args.radius

    workers = args.workers

    # Initialize logging
    logging.basicConfig(filename=Path(local, "processing.log"),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    if mode == "HDF_TO_PARQUET":
        if path_only:
            hdf_to_parquet_mode(main_dir, ccds, single_ccd, bands, single_band, workers, logger)
        else:
            logger.error("This mode is only available for single directories, not global ones. Aborting.")
    elif mode == "DCMP_TO_PARQUET":
        if path_only:
            dcmp_to_parquet_mode(main_dir, ccds, single_ccd, bands, single_band, workers, logger)
        else:
            logger.error("This mode is only available for single directories, not global ones. Aborting.")
    elif mode == "CREATE_CCD_BAND_DB":
        if path_only:
            logger.error("This mode is only available for global directories, not single ones. Aborting.")
        else:
            create_db_ccd_band_mode(main_dir, ccds, single_ccd, bands, single_band, workers, glob_name, logger)
    elif mode == "MASTER_CATALOG_CCD":
        if path_only:
            logger.error("This mode is only available for global directories, not single ones. Aborting.")
        else:
            master_catalog_ccd_mode(main_dir, ccds, single_ccd, bands, single_band, workers, glob_name, logger)
    elif mode == "LIGHTCURVE":
        if path_only:
            logger.error("This mode is only available for global directories, not single ones. Aborting.")
        else:
            lightcurve_mode(main_dir,input_path, input_ra, input_dec, input_radius, output_dir, glob_name, logger)
    elif mode == "REMOVE_DUPLICATES":
        if path_only:
            logger.error("This mode is only available for global directories, not single ones. Aborting.")
        else:
            remove_duplicates_mode(main_dir, ccds, single_ccd, bands, single_band, workers, glob_name, logger)

if __name__ == "__main__":
    main()