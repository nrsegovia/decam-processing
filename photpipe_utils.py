from typing import Tuple
from pathlib import Path
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import gc
import re
from astropy.io import fits
from astropy.wcs import WCS
from astropy.table import Table
import logging

"""
Functionality related to processing photpipe output into parquet catalogues
"""

def process_photpipe_dir(directory_path: Path, ccd: int, bands: list, logger: logging.Logger, workers: int):
    
    path_dictionary = {band : [] for band in bands}
    directory_path = Path(directory_path)
    dcmp_files = [x for x in directory_path.glob("*.dcmp")]
    # Separate by band
    for dcmp_file in dcmp_files:
        check_band = re.search(r"/*[.][griz][.]", dcmp_file, flags=0)
        if check_band:
            current_band = check_band.group(0)[1] # This retrieves the matched pattern and selects the relevant band
            path_dictionary[current_band].append(dcmp_file)
        else:
            logger.error(f"{dcmp_file} does not match the expected format. Skipping.")
    
    for selected_band in path_dictionary.keys():
        output_directory = Path(directory_path, selected_band)
        output_directory.mkdir(parents=True, exist_ok=True)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(photpipe_to_parquet, dcmp, ccd, selected_band, index) for index, dcmp in enumerate(path_dictionary[selected_band], start = 1)]

        info_entries = []

        for future in futures:
            results = future.result()
            info_entries.append(results)
        
    # The following is a reminder: must create the Header.info file, but using the list of rows.
    # Create dataframe to store the information collected form headers
    df2 = pd.DataFrame()
    df2['Original'] = infoKeeping[0]
    df2['File'] = infoKeeping[1]
    for x in range(extraInfoCols):
        df2[infoColumnsToKeep[x]] = infoKeeping[2 + x]
    df2.to_csv(os.path.join(finalPath, 'Header.info'), index = False)
    print(f'CCD {ccd} files successfully converted into HDF files.')

        return success, fail, errors, messages
            


def photpipe_to_parquet(path, ccd, band, counter, columns = ['RA', 'Dec', 'M', 'dM', 'flux', 'dflux','type'], infoColumns = ["MJD-OBS", "ZPTMAG", "M10SIGMA"]):

    extraInfoCols = len(infoColumns)
    simplifiedName = f'{ccd}.{band}.{counter}.parquet'
    infoKeeping = f"{path},{simplifiedName},{counter},"
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
    initial = Table.read(path, format = 'ascii.no_header', data_start = 1, names = ['Xpos','Ypos','M','dM','flux','dflux','type','peakflux','sigx','sigxy','sigy','sky','chisqr','class','FWHM1','FWHM2','FWHM','angle','extendedness','flag','mask','Nmask','RA','Dec'])
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
    
    return infoKeeping[:-1]