from typing import Tuple
from pathlib import Path
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor
import numpy as np

"""
Functionality related to converting from fits to parquet file format.
"""

def HDF5_file_to_parquet(hdf_file: Path, parquet_file: Path) -> int:
    """Converts single HDF file to parquet format"""
    status_success = 0
    try:
        df = pd.read_hdf(hdf_file)
        df.to_parquet(parquet_file, index=False)

        # Verify conversion
        df_parquet = pd.read_parquet(parquet_file)
        if df.equals(df_parquet):
            hdf_file.unlink()  # Delete HDF5 file
            status_success = 1
    except Exception:
        status_success = -1
    
    return status_success

def HDF5_directory_to_parquet(directory_path: Path, logger: logging.Logger, workers: int = 1) -> Tuple:
    """Convert HDF5 files in a given directory to Parquet format.
    Useful for moving away from the old storage system.
    WARNING: This removes old HDF5 files"""
    
    directory_path = Path(directory_path)
    hdf_files = [x for x in directory_path.glob("*.hdf")]
    parquet_files = [x.with_suffix('.parquet') for x in hdf_files]
    file_pairs = list(zip(hdf_files, parquet_files))

    # Convert HDF5 to Parquet
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(HDF5_file_to_parquet, hdf, pq) for hdf, pq in file_pairs]

    results = np.array([future.result() for future in futures])
    success = sum(results > 0)
    fail = sum(results == 0)
    errors = sum(results < 0)

    return success, fail, errors