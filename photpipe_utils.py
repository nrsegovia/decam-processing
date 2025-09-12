from pathlib import Path
import logging
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import re
from astropy.io import fits
from astropy.wcs import WCS
from astropy.table import Table
from astropy.io.fits.verify import VerifyWarning
import logging
import warnings

"""
Functionality related to processing photpipe output into parquet catalogues
"""

def process_photpipe_dir(directory_path: Path, ccd: int, bands: str, logger: logging.Logger, workers: int, show_warnings: bool = False):
    if not show_warnings:
        warnings.simplefilter('ignore', category=VerifyWarning)#hide fits warnings due to differences in size
        warnings.simplefilter('ignore', category=UserWarning)
    path_dictionary = {band : [] for band in bands}
    directory_path = Path(directory_path)
    dcmp_files = [x for x in directory_path.glob("*.dcmp")]
    # Separate by band
    for dcmp_file in dcmp_files:
        check_band = re.search(r"/*[.][griz][.]", str(dcmp_file), flags=0)
        if check_band:
            current_band = check_band.group(0)[1] # This retrieves the matched pattern and selects the relevant band
            # Only process specified bands
            if current_band in bands:
                path_dictionary[current_band].append(dcmp_file)
        else:
            logger.error(f"{dcmp_file} does not match the expected format. Skipping.")
    # Header.info header
    info_header = "Original,File,"
    info_columns = ["MJD-OBS", "ZPTMAG", "M10SIGMA"]
    for x in info_columns:
        info_header += f"{x},"
    info_header = info_header[:-1] + "\n"
    successes = []
    messages = []
    total_dcmp = 0
    for selected_band in path_dictionary.keys():
        output_directory = Path(directory_path, selected_band)
        output_directory.mkdir(parents=True, exist_ok=True)
        these_dcmp = path_dictionary[selected_band]
        total_dcmp += len(these_dcmp)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(photpipe_to_parquet, dcmp, ccd, selected_band, index, infoColumns = info_columns) for index, dcmp in enumerate(these_dcmp, start = 1)]

        info_entries = [info_header]

        for future in futures:
            results = future.result()
            info_row = results[0]
            if info_row != "":
                info_entries.append(info_row)
            successes.append(results[1])
            messages.append(results[2])
        # Create csv file to store the information collected form headers
        with open(Path(output_directory, 'Header.info'), "w") as info_file:
            info_file.writelines(info_entries)

    successes = np.array(successes)
    ok = sum(successes > 0)
    fail = sum(successes < 0)

    return ok, fail, total_dcmp, messages
            


def photpipe_to_parquet(path, ccd, band, counter, columns = ['RA', 'Dec', 'M', 'dM', 'flux', 'dflux','type'], infoColumns = ["MJD-OBS", "ZPTMAG", "M10SIGMA"]):

    status_success = -1
    message = ""
    extraInfoCols = len(infoColumns)
    simplifiedName = f'{ccd}.{band}.{counter}.parquet'
    infoKeeping = f"{path},{simplifiedName},{counter},"
    try:
        # Read header to acquire important information
        hdul = fits.open(path) 
        hdr = hdul[0].header

        for x in range(extraInfoCols):
            content = hdr.get(infoColumns[x])
            if content: 
                #Armin Rest pointed out that in some cases computation fails and therefore some elements are not listed. So we avoid that error by checking beforehand
                infoKeeping += f"{content}," # hdr[infoColumns[x]]
            else:
                # As 'missing value' flag
                infoKeeping += f"{-99},"
        
        w = WCS(hdr)
        hdul.close()

        # Load data as pandas data frame (convenience)
        initial = Table.read(path, format = 'ascii.no_header',
                            data_start = 1,
                            names = ['Xpos','Ypos','M','dM','flux',
                                    'dflux','type','peakflux','sigx',
                                    'sigxy','sigy','sky','chisqr',
                                    'class','FWHM1','FWHM2','FWHM',
                                    'angle','extendedness','flag',
                                    'mask','Nmask','RA','Dec'])
        
        initial.remove_columns(['RA', 'Dec'])
        df = initial.to_pandas()

        # Compute coordinates, as they are not computed by photpipe
        coords2 = np.c_[df['Xpos'].values,df['Ypos'].values]
        sky = w.all_pix2world(coords2,1)
        df['RA'] = sky[:,0]
        df['Dec'] = sky[:,1]

        # Reorder columns and keep only specified ones
        df = df[columns] 

        # Save file
        df.to_parquet(Path(path.stem, band, simplifiedName), index = False)
        out_info_row = infoKeeping[:-1] + "\n"
        status_success = 1
    except Exception as e:
        message = str(e)
        out_info_row = ""

    return out_info_row, status_success, message