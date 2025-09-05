import logging
from parquet_utils import *
from pathlib import Path

'''
Main program. Call other functions from here
'''

if __name__ == "__main__":
    local = Path(__file__).parent
    # Setup logging
    logging.basicConfig(filename=Path(local, "processing.log"),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Converting HDF files to Parquet...")
    success, fail, errors = HDF5_directory_to_parquet(Path("C:\\Work\\dataannotation\\matcher_boosted\\testing"), logger, 4)
    logger.info(f"Finished with {success} succesfully verified, {fail} non-verified, and {errors} error file(s).")