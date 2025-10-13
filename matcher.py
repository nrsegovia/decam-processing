# import logging
from pathlib import Path
import tempfile
from constants import *
import subprocess
import pandas as pd
import numpy as np
from utils import *
import concurrent.futures
from multiprocessing import Pool, Process, Queue
# import pyarrow.parquet as pq
# import pyarrow
from typing import Tuple
import json
from datetime import datetime
# It seems that using sqlalchemy instead would make the process faster?
import sqlite3

def writer_process(logger, write_queue, out_dir, ccd, bands):
    """
    Single writer process - handles all database writes.
    This ensures no database locking issues.
    """
    db_path = Path(out_dir, f"{ccd}.db")
    db = sqlite3.connect(db_path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    
    # Initialize all band tables
    for band in bands:
        initialize_band_table(db, band)
    
    processed_count = 0
    
    while True:
        item = write_queue.get()
        
        if item is None:  # shutdown signal
            break
        
        band, df = item
        
        # Write to database
        df.to_sql(f'lightcurves_{band}', db, if_exists='append',
                  index=False, method='multi', chunksize=10000)
        db.commit()
        
        processed_count += 1
        logger.info(f"Written band {band} to database ({processed_count} batches total)")
    
    db.close()
    logger.info(f"Writer process finished. Total batches written: {processed_count}")

def initialize_band_table(db, band):
    db.execute(f"""
                CREATE TABLE IF NOT EXISTS lightcurves_{band} (
                    ID TEXT NOT NULL,
                    MJD REAL NOT NULL,
                    M REAL,
                    dM REAL,
                    flux REAL,
                    dflux REAL,
                    type INTEGER,
                    Separation REAL,

                    PRIMARY KEY (ID, MJD)
                )
            """)
            
    # Create indexes for fast queries
    db.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_source 
                ON lightcurves_{band}(ID)
            """)
            
    db.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_mjd 
                ON lightcurves_{band}(MJD)
            """)
            
    db.commit()

def bulk_insert(db, df, band):
    """
    Efficiently insert large batch of observations.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Must have columns: ID, MJD, M, dM, flux, dflux, 
                            type, Separation
    """
    df.to_sql(f'lightcurves_{band}', db, if_exists='append', 
                index=False, method='multi', chunksize=10000)
    db.commit()
# Call topcat/stilts, no multiprocessing customization as I do not know how stilts scales.

def match_list_of_files(logger, path_list, band):
    #Just use the first one as starting point
    logger.info(f"Starting worker for band {band}")
    try:
        master_cat = None
        next_start_id = 1
        prev_start_id = -1
        cols_to_drop = ["RA_1", "Dec_1", "RA_2", "Dec_2"]
        total_files = len(path_list)
        deciles = np.linspace(0, total_files, 11).astype(int) # This sets when to print
        for idx, cat_to_match in enumerate(path_list):
            if idx == 0:
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    temp_master = Path(tmp_file.name)

                master_cat = pd.read_parquet(cat_to_match)
                size_cat = len(master_cat)
                prev_start_id = next_start_id
                next_start_id = size_cat + 1
                # Columns here must be checked, but assuming that required cols are available
                master_cat["Separation"] = 0.0 # Base coord
                master_cat["ID"] = range(prev_start_id, next_start_id)
                zpt = get_from_header(path_list[0], "ZPTMAG")
                mjd = get_from_header(path_list[0], "MJD-OBS")
                # Apply ZP and add date column
                master_cat['M'] += zpt
                master_cat['MJD'] = mjd
                to_insert = master_cat.copy().drop(columns=["RA", "Dec"])
                worker_queue.put((band, to_insert)) # This comes from the global variable
                # bulk_insert(db, to_insert, band)
                master_cat = master_cat[["ID", "RA", "Dec"]]
                #Save mastercat
                master_cat.to_parquet(temp_master, index=False)
                
            else:
                # Update input catalogue and save as temp file
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    temp_input = Path(tmp_file.name)
                in_cat = pd.read_parquet(cat_to_match)
                zpt = get_from_header(cat_to_match, "ZPTMAG")
                mjd = get_from_header(cat_to_match, "MJD-OBS")
                # Apply ZP and add date column
                in_cat['M'] += zpt
                in_cat['MJD'] = mjd
                in_cat.to_parquet(temp_input, index=False)

                temp_master_old = temp_master
                
                # Match
                matched = stilts_crossmatch_pair(logger,temp_master_old, temp_input)

                # Remove temp files
                temp_master_old.unlink()
                temp_input.unlink()

                missing_id = pd.isna(matched["ID"])
                missing_new = pd.isna(matched["RA_1"])
                total_missing = missing_id.sum()
                prev_start_id = next_start_id
                next_start_id = prev_start_id + total_missing
                matched.loc[missing_id, "ID"] = range(prev_start_id, next_start_id)
                to_append = matched[missing_new].copy()
                to_append.drop(columns=cols_to_drop, inplace=True)
                worker_queue.put((band, to_append)) # This comes from the global variable
                # bulk_insert(db, to_append, band)
                # Now update master cat
                nan_ra = matched["RA_1"].isna()
                matched.loc[nan_ra, "RA_1"] = matched.loc[nan_ra, "RA_2"]
                nan_dec = matched["Dec_1"].isna()
                matched.loc[nan_dec, "Dec_1"] = matched.loc[nan_dec, "Dec_2"]

                matched.rename(columns={"RA_1" : "RA", "Dec_1" : "Dec"}, inplace=True)
                # Dropping columns other than ID and coord
                matched = matched[["ID", "RA", "Dec"]]

                # New master cat temp file due to chache reasons
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    temp_master = Path(tmp_file.name)
                matched.to_parquet(temp_master, index=False)
            if idx+1 in deciles:
                logger.info(f"{idx+1} out of {total_files} processed. Current master catalogue contains {len(matched)} rows.")
        final_df = pd.read_parquet(temp_master)
    except Exception as e:
        logger.error(e)
        final_df = None
    finally:
        try:
            temp_master.unlink()
            logger.info(f"Band {band} processing complete")
            return final_df
        except Exception as e:
            logger.error(e)

def stilts_crossmatch_pair(logger,  catalog1_path: Path, catalog2_path: Path) -> pd.DataFrame:
        """Run STILTS crossmatch between two catalogs."""
        # logger.info(f"Crossmatching {catalog1_path.name} with {catalog2_path.name}")
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            temp_output = Path(tmp_file.name)
        
        try:
            cmd = [
                'java', '-jar', STILTS,
                '-stilts', 'tmatch2',
                f"in1={catalog1_path}", 'ifmt1=parquet',
                f"in2={catalog2_path}", 'ifmt2=parquet',
                'matcher=sky',
                f"values1={CROSSMATCH['col1_ra']} {CROSSMATCH['col1_dec']}",
                f"values2={CROSSMATCH['col2_ra']} {CROSSMATCH['col2_dec']}",
                f"params={CROSSMATCH['radius']}",
                f"join={CROSSMATCH['join_type']}",
                'omode=out',
                f'out={temp_output}', 'ofmt=parquet'
            ]
            
            # logger.info(f"Running STILTS command...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            df = pd.read_parquet(temp_output)
            # logger.info(f"Crossmatch completed: {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except subprocess.CalledProcessError as e:
            logger.error(f"STILTS command failed: {e.stderr}")
            raise
        finally:
            try:
                temp_output.unlink()
            except:
                pass

def stilts_crossmatch_N(logger,  path_dictionary: dict, keepcols: str = '$3 $4 $6 $8') -> pd.DataFrame:
        #ID RA Dec
        """Run STILTS crossmatch between N catalogs."""
        logger.info(f"Crossmatching catalogs: {path_dictionary.values()}")
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            temp_output = Path(tmp_file.name)
        
        try:
            these_keys = list(path_dictionary.keys())
            cmd = [
                'java', '-jar', STILTS,
                '-stilts', 'tmatchn']
            for index, key in enumerate(these_keys, start=1):
                cmd += [f"in{index}={path_dictionary[key]}", f'ifmt{index}=parquet',
                        f"values{index}=RA Dec", f"join{index}=always",
                        f"icmd{index}=keepcols '{keepcols}'",
                        f"suffix{index}=_{key}"]

            cmd += ["fixcols=all",
                f'nin={len(path_dictionary)}',
                'matcher=sky',
                'multimode=pairs',
                f"params={CROSSMATCH['radius_matchn']}",
                'omode=out',
                f'out={temp_output}', 'ofmt=parquet'
            ]
            
            logger.info(f"Running STILTS command...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            df = pd.read_parquet(temp_output)
            # Create single sky coordinates based on wavelength importance (blue > red)
            df['RA'] = df[[f"RA_{x}" for x in these_keys]].bfill(axis=1).iloc[:, 0]
            df['Dec'] = df[[f"Dec_{x}" for x in these_keys]].bfill(axis=1).iloc[:, 0]
            
            logger.info(f"Crossmatch completed: {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except subprocess.CalledProcessError as e:
            logger.error(f"STILTS command failed: {e.stderr}")
            raise
        finally:
            try:
                temp_output.unlink()
            except:
                pass

def stilts_crossmatch_external(logger,  in_path: Path, master_path: Path, inra, indec, match_radius) -> pd.DataFrame:
        """Run STILTS crossmatch between two catalogs."""
        logger.info(f"Crossmatching {in_path.name} with {master_path.name}")
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            temp_output = Path(tmp_file.name)
        
        try:
            cmd = [
                'java', '-jar', STILTS,
                '-stilts', 'tmatch2',
                f"in1={in_path}", 'ifmt1=parquet',
                f"in2={master_path}", 'ifmt2=parquet',
                'matcher=sky',
                f"values1={inra} {indec}",
                f"values2=RA Dec",
                f"params={match_radius}",
                f"join=1and2",
                'omode=out',
                f'out={temp_output}', 'ofmt=parquet'
            ]
            
            logger.info(f"Running STILTS command...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            df = pd.read_parquet(temp_output)
            logger.info(f"Crossmatch completed: {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except subprocess.CalledProcessError as e:
            logger.error(f"STILTS command failed: {e.stderr}")
            raise
        finally:
            try:
                temp_output.unlink()
            except:
                pass


def create_db_ccd_band(logger, bands, field_paths, ccd, out_dir):
    # Probably must add method to check what ccd, band and field? has been completed
    # Otherwise the database must be deleted every time a new dataset wants to be added...

    write_queue = Queue()
    writer = Process(
            target=writer_process,
            args=(write_queue, out_dir, ccd)
        )
    writer.start()

    def init_worker(q):
        # To make the queue accessible from all processes
        global worker_queue
        worker_queue = q

    # Process bands in parallel using Pool
    args_list = []

    for band in bands:
        relevant_paths = [Path(x, str(ccd), band) for x in field_paths]
        all_paths = []
        for this_path in relevant_paths:
            all_paths += list(this_path.glob("*.parquet"))
        args_list.append((logger, all_paths, band))

    with Pool(len(bands), initializer=init_worker, initargs=(write_queue, )) as pool:
        # starmap returns list of results in order
        results = pool.starmap(match_list_of_files, args_list)
    
    for band, df in results:
        df.to_parquet(Path(out_dir, f"{ccd}.{band}.master.catalogue.parquet"), index = False)        
    
    # Stop writer
    write_queue.put(None)
    writer.join()

    logger.info("All bands processed and written to database")




def create_ccd_master_catalog(logger, glob_name, field_paths, ccd, out_dir):
    # Rework to use tmatch... hopefully the new band catalogues contain thousands
    # of entries, not millions.
    # Assumes that ccd master catalogs have been created, no other option.
    paths_to_master_cats = {x : Path(out_dir, str(ccd), f"{ccd}.{x}.master.catalogue.parquet") for x in "griz"}
    matched = stilts_crossmatch_N(logger, paths_to_master_cats)
    # Create and save CCD edges
    json_file = Path(out_dir, '..', "fields_info.json")
    if json_file.exists():
        with open(json_file, 'r') as f:
            json_data = json.load(f)
    else:
        json_data = {}
    if glob_name not in json_data:
        json_data[glob_name] = {}
    if ccd not in json_data[glob_name]:
        json_data[glob_name][ccd] = {}

    json_data[glob_name][ccd]["RA"] = [matched.RA.min(), matched.RA.max()]
    json_data[glob_name][ccd]["Dec"] = [matched.Dec.min(), matched.Dec.max()]
    with open(json_file, 'w') as f:
        json.dump(json_data, f, indent=3)

    # Save catalogue
    matched.to_parquet(Path(out_dir, str(ccd), f"{ccd}.final.catalogue.parquet"), index = False)

def extract_light_curves(logger, glob_name, field_paths, ccd, out_dir, to_match_cat, ra_str, dec_str, match_radius, save_dir):
    # Must be reworked to according to the following steps:
    # Check if any input source is located within the data limits
    # Perform sql search... parallely perhaps? Look for most efficient method.
    pass
    # master_cat = Path(out_dir, str(ccd), f"{ccd}.final.catalogue.parquet")
    # # Create to_match_cat from df
    # with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
    #     df_file = tmp_file.name
    # to_match_cat.to_parquet(df_file, index = False)
    # # The command below should probably be merged with stilts_crossmatch_pair
    # # But it requires some refactoring...
    # matches = stilts_crossmatch_external(logger, Path(df_file), master_cat, ra_str, dec_str, match_radius)
    # # Delete temp file
    # Path(df_file).unlink()

    # total_matched = len(matches)
    # new_cols = matches.columns.to_list()
    # # Default output is ['ID_1', 'Type', 'Subtype', 'RA_1', 'Dec_1', 'I', 'V', 'V_I', 'P_1',
    # #    'REMARKS', 'RA_g', 'Dec_g', 'ID_g', 'nobs_g', 'RA_r', 'Dec_r', 'ID_r',
    # #    'nobs_r', 'Separation_1', 'RA_i', 'Dec_i', 'ID_i', 'nobs_i',
    # #    'Separation_1a', 'RA_z', 'Dec_z', 'ID_z', 'nobs_z', 'Separation_2',
    # #    'RA_2', 'Dec_2', 'ID_2', 'Separation']
    # id_col = "ID_2" if "ID_2" in new_cols else "ID"

    # if total_matched > 0:
    #     logger.info(f"{total_matched} match(es) found. Creating lightcurves and cross-matched catalogue.")
    #     timestamp_iso8601 = datetime.now().isoformat().replace(':', '-')
    #     cat_path = Path(save_dir, f"{timestamp_iso8601}_result.csv")
    #     matches.to_csv(cat_path, index=False)
    #     logger.info(f"Catalogue created at {cat_path}")
    #     Probably better to preload band catalogues here and pass them as arguments.
    #     Same idea for batches
    #     # Collect matches
    #     for match in matches.itertuples():
    #         # Start by checking which bands have matches
    #         band_keys = "griz"
    #         output = []
    #         current_id = getattr(match, id_col)
    #         for band in band_keys:
    #             band_id = getattr(match, f"ID_{band}")
    #             if pd.isna(band_id):
    #                 # Nothing to do here
    #                 pass
    #             else:
    #                 output.append(retrieve_cols_from_batches(band, band_id, field_paths, ccd, glob_name, out_dir))
    #         df_to_store = pd.concat(output)
    #         df_to_store.to_csv(Path(save_dir, f"{ccd}.{current_id}.csv"), index = False)
    # else:
    #     logger.info("No matches found. No curves have been extracted.")
    # # Check whether matches is empty
    # # If not empty, retrieve data from relevant places