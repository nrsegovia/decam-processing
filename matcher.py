# import logging
from pathlib import Path
import tempfile
from constants import *
import subprocess
import pandas as pd
import numpy as np
from utils import *
from multiprocessing import Pool, Process, Manager
import json
from datetime import datetime
import duckdb
import logging
from logging.handlers import QueueHandler

def get_rows_by_ids(db_path, table_name, ids):
    """
    Retrieve rows from a DuckDB table matching the given IDs.
    Assumes ID column is named 'ID'.

    Parameters
    ----------
    db_path : str
        Path to the DuckDB database file.
    table_name : str
        Name of the table to query.
    ids : list, tuple, np.ndarray, or scalar
        One or more IDs to match.

    Returns
    -------
    pd.DataFrame
        DataFrame containing all matching rows from the database.
        Empty DataFrame if no matches found.
    """
    # Ensure ids is iterable
    if not isinstance(ids, (list, tuple, np.ndarray)):
        ids = [ids]

    # Handle empty list early to avoid SQL syntax error
    if len(ids) == 0:
        return pd.DataFrame()

    # Open connection
    con = duckdb.connect(db_path)

    # Create placeholders: DuckDB supports parameter substitution with '?'
    placeholders = ','.join(['?'] * len(ids))
    query = f"SELECT * FROM {table_name} WHERE ID IN ({placeholders})"

    # Execute the query with parameters
    df = con.execute(query, ids).df()

    # Close the connection
    con.close()
    return df

def writer_process(logger, write_queue, out_dir, ccd, band):
    """
    Single writer process for one band - writes to separate DuckDB file.
    
    Args:
        logger: Logger instance
        write_queue: Queue for receiving data to write
        out_dir: Output directory
        ccd: CCD identifier
        band: Band name
    """
    db_path = Path(out_dir, f"{ccd}.{band}.duckdb")
    db = duckdb.connect(str(db_path))
    
    # Optimize DuckDB for bulk inserts
    db.execute("SET memory_limit='10GB'")  # Adjust based on your system
    db.execute("SET threads TO 6")  # Use multiple threads
    db.execute("SET preserve_insertion_order=false")  # Faster inserts
    
    # Initialize table
    initialize_band_table(db, band)
    
    # Duplicate tracking
    duplicate_log_path = Path(out_dir, f"{ccd}.{band}.duplicates.log")
    
    # Buffer for batching
    buffer = []
    BATCH_SIZE = 250_000  # Accumulate rows before writing
    processed_count = 0
    total_duplicates = 0
    total_rows_written = 0
    
    def flush_buffer():
        """Write accumulated data to database"""
        nonlocal total_duplicates, total_rows_written
        
        if not buffer:
            return 0
        
        combined_df = pd.concat(buffer, ignore_index=True)
        db.register('combined_df', combined_df)

        # Check for duplicates against existing data
        duplicate_check = db.execute(f"""
            SELECT 
                c.ID, c.MJD, c.M, c.flux,
                l.M as existing_M, l.flux as existing_flux
            FROM combined_df c
            INNER JOIN lightcurves_{band} l 
            ON c.ID = l.ID AND c.MJD = l.MJD
        """).fetchdf()
        
        if len(duplicate_check) > 0:
            total_duplicates += len(duplicate_check)
            
            # Log duplicate details
            with open(duplicate_log_path, 'a') as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"Band: {band} | Timestamp: {pd.Timestamp.now()}\n")
                f.write(f"Found {len(duplicate_check)} duplicate(s)\n")
                f.write(f"{'='*80}\n")
                
                for _, row in duplicate_check.iterrows():
                    delta_m = abs(row['M'] - row['existing_M'])
                    delta_flux = abs(row['flux'] - row['existing_flux'])
                    f.write(f"ID={row['ID']}, MJD={row['MJD']:.6f}\n")
                    f.write(f"  New:      M={row['M']:.4f}, flux={row['flux']:.2f}\n")
                    f.write(f"  Existing: M={row['existing_M']:.4f}, flux={row['existing_flux']:.2f}\n")
                    f.write(f"  Delta:    ΔM={delta_m:.6f}, Δflux={delta_flux:.2f}\n\n")
            
            logger.warning(f"[{band}] Found {len(duplicate_check)} duplicate(s). "
                          f"Details logged to {duplicate_log_path}")
        
        # Insert only non-duplicates
        rows_before = db.execute(f"SELECT COUNT(*) FROM lightcurves_{band}").fetchone()[0]
        
        db.execute(f"""
            INSERT INTO lightcurves_{band} 
            SELECT * FROM combined_df
            WHERE NOT EXISTS (
                SELECT 1 FROM lightcurves_{band} l
                WHERE l.ID = combined_df.ID AND l.MJD = combined_df.MJD
            )
        """)
        
        rows_after = db.execute(f"SELECT COUNT(*) FROM lightcurves_{band}").fetchone()[0]
        rows_inserted = rows_after - rows_before
        total_rows_written += rows_inserted
        
        buffer.clear()
        return rows_inserted
    
    # Main processing loop
    while True:
        item = write_queue.get()
        
        if item is None:  # Shutdown signal
            # Flush remaining buffer
            rows = flush_buffer()
            if rows > 0:
                logger.info(f"[{band}] Final flush: {rows} rows inserted")
            break
        
        band_name, df = item
        buffer.append(df)
        
        # Check if buffer is large enough to flush
        current_size = sum(len(d) for d in buffer)
        if current_size >= BATCH_SIZE:
            rows = flush_buffer()
            processed_count += 1
            logger.info(f"[{band}] Batch #{processed_count}: {rows} rows inserted "
                       f"(total: {total_rows_written})")
    
    db.close()
    
    # Final summary
    if total_duplicates > 0:
        logger.warning(f"[{band}] Writer finished. Total rows written: {total_rows_written}, "
                      f"Duplicates found: {total_duplicates}. See {duplicate_log_path}")
    else:
        logger.info(f"[{band}] Writer finished. Total rows written: {total_rows_written}. "
                   f"No duplicates found.")

def initialize_band_table(db, band):
    """
    Create lightcurve table for a band.
    
    Args:
        db: DuckDB connection
        band: Band name
    """
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS lightcurves_{band} (
            ID INTEGER NOT NULL,
            MJD DOUBLE NOT NULL,
            M DOUBLE NOT NULL,
            dM DOUBLE NOT NULL,
            flux DOUBLE NOT NULL,
            dflux DOUBLE NOT NULL,
            type INTEGER NOT NULL,
            Separation DOUBLE NOT NULL
        )
    """)


def create_indexes(db_path, band, logger):
    """
    Create indexes after all data is inserted (much faster than during insert).
    
    Args:
        db_path: Path to DuckDB database file
        band: Band name
        logger: Logger instance
    """
    db = duckdb.connect(str(db_path))
    
    logger.info(f"[{band}] Creating index on ID...")
    db.execute(f"CREATE INDEX IF NOT EXISTS idx_source_{band} ON lightcurves_{band}(ID)")
    
    logger.info(f"[{band}] Creating index on MJD...")
    db.execute(f"CREATE INDEX IF NOT EXISTS idx_mjd_{band} ON lightcurves_{band}(MJD)")
    
    logger.info(f"[{band}] Creating unique index on (ID, MJD)...")
    db.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_primary_{band} 
        ON lightcurves_{band}(ID, MJD)
    """)
    
    db.close()
    logger.info(f"[{band}] Indexes created successfully")


# Call topcat/stilts, no multiprocessing customization as I do not know how stilts scales.

def match_list_of_files(path_list, band):
    """
    Process and match a list of catalog files for a single band.
    Sends data to band-specific queue as it processes.
    
    Args:
        logger: Logger instance
        path_list: List of paths to parquet catalog files
        band: Band name
        
    Returns:
        tuple: (band, final_master_dataframe)
    """
    logger = logging.getLogger()
    logger.info(f"Starting worker for band {band}")
    final_df = None
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
                zpt = get_from_header(cat_to_match, "ZPTMAG")
                mjd = get_from_header(cat_to_match, "MJD-OBS")
                # Apply ZP and add date column
                master_cat['M'] += zpt
                master_cat['MJD'] = mjd
                to_insert = master_cat.copy().drop(columns=["RA", "Dec"])
                worker_queues[band].put((band, to_insert)) # This comes from the global variable
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

                missing_id = pd.isna(matched["ID"]) # This is true where no ID is found, hence new matches
                missing_ra = pd.isna(matched["RA_2"]) # True where no match in new cat
                
                # Pre-existing matches
                to_append_pre = matched[(~missing_id) & (~missing_ra)].copy() # the ~ applies a "not"
                to_append_pre.drop(columns=cols_to_drop, inplace=True)
                to_db = [to_append_pre]

                # If any new matches
                if sum(missing_id) > 0:
                    total_missing = missing_id.sum()
                    prev_start_id = next_start_id
                    next_start_id = prev_start_id + total_missing
                    matched.loc[missing_id, "ID"] = range(prev_start_id, next_start_id)
                    to_append = matched[missing_id].copy()
                    to_append.drop(columns=cols_to_drop, inplace=True)
                    to_append["Separation"] = 0.0
                    to_db.append(to_append)

                to_db = pd.concat(to_db)
                worker_queues[band].put((band, to_db)) # This comes from the global variable
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
                logger.info(f"{band}-band: {idx+1} out of {total_files} processed. Current master catalogue contains {len(matched)} rows.")
        final_df = pd.read_parquet(temp_master)
    except Exception as e:
        logger.error(f"Error processing {band}-band: {e}")
        final_df = None
    finally:
        try:
            temp_master.unlink()
            logger.info(f"Band {band} processing complete")
        except Exception as e:
            logger.error(f"Error for band {band}: {e}")
    return band, final_df

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

def stilts_crossmatch_N(logger,  path_dictionary: dict) -> pd.DataFrame:
        #ID RA Dec
        """Run STILTS crossmatch between N catalogs."""
        path_string = '\n'.join([str(x) for x in path_dictionary.values()])
        logger.info(f"Crossmatching catalogs: {path_string}")
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            temp_output = Path(tmp_file.name)
        
        try:
            these_keys = list(path_dictionary.keys())
            cmd = [
                'java', '-Xmx128G', '-jar', STILTS,
                '-stilts', 'tmatchn']
            for index, key in enumerate(these_keys, start=1):
                cmd += [f"in{index}={path_dictionary[key]}", f'ifmt{index}=parquet',
                        f"values{index}=RA Dec", f"join{index}=always",
                        # f"icmd{index}=keepcols '{keepcols}'",
                        f"suffix{index}=_{key}"]

            cmd += ["fixcols=all",
                f'nin={len(path_dictionary)}',
                'matcher=sky',
                'multimode=pairs',
                f"params={CROSSMATCH['radius']}",
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
            # Create "Global" ID
            df['ID'] = range(1, len(df) + 1)
            
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
                f"find=all",
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

def init_worker(log_queue, queues):
    # Clear existing handlers (important to avoid duplicates)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    # Add a QueueHandler that sends logs to the main process
    qh = QueueHandler(log_queue)
    logger.addHandler(qh)

    # Also store other shared objects like your queues
    global worker_queues
    worker_queues = queues

def create_db_ccd_band(logger, log_queue, listener, bands, field_paths, ccd, out_dir):
    # Probably must add method to check what ccd, band and field? has been completed
    # Otherwise the database must be deleted every time a new dataset wants to be added...
    Path(out_dir).mkdir(exist_ok=True, parents=True)

    manager = Manager()
    queues = {}
    writers = {}

    for band in bands:
        queues[band] = manager.Queue(maxsize=0)
        writers[band] = Process(
            target=writer_process,
            args=(logger, queues[band], out_dir, ccd, band)
        )
        writers[band].start()
        logger.info(f"Started writer process for {band}-band")

    # Process bands in parallel using Pool
    args_list = []

    try:
        for band in bands:
            relevant_paths = [Path(x, str(ccd), band) for x in field_paths]
            all_paths = []
            for this_path in relevant_paths:
                all_paths += list(this_path.glob("*.parquet"))
            args_list.append((all_paths, band))

        with Pool(len(bands), initializer=init_worker, initargs=(log_queue, queues)) as pool:
            # starmap returns list of results in order
            results = pool.starmap(match_list_of_files, args_list)
        
        for band, df in results:
            if df is not None:
                master_cat_path = Path(out_dir, f"{ccd}.{band}.master.catalogue.parquet")
                df.to_parquet(master_cat_path, index=False)
                logger.info(f"Saved master catalogue for {band}-band: {len(df)} sources")

    except Exception as e:
        logger.error(f"Error in processing: {e}")
        raise
    
    finally:
        # Stop all writer processes
        for band in bands:
            queues[band].put(None)
        
        for band in bands:
            writers[band].join()
            logger.info(f"Writer process for {band}-band finished")
        
        # Create indexes for all databases
        logger.info("Creating indexes for all bands...")
        for band in bands:
            db_path = Path(out_dir, f"{ccd}.{band}.duckdb")
            if db_path.exists():
                create_indexes(db_path, band, logger)
        
        logger.info("All bands processed and written to DuckDB databases")




def create_ccd_master_catalog(logger, glob_name, field_paths, ccd, out_dir):
    # Rework to use tmatch... hopefully the new band catalogues contain thousands
    # of entries, not millions.
    # Assumes that ccd master catalogs have been created, no other option.
    paths_to_master_cats = {x : Path(out_dir, f"{ccd}.{x}.master.catalogue.parquet") for x in "griz"}
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
    matched.to_parquet(Path(out_dir, f"{ccd}.final.catalogue.parquet"), index = False)

def extract_light_curves(logger, glob_name, field_paths, ccd, out_dir, to_match_cat, ra_str, dec_str, match_radius, save_dir):
    # Must be reworked to according to the following steps:
    # Check if any input source is located within the data limits
    # Crossmatch external with final master
    # Perform sql search... parallely perhaps? Look for most efficient method.
    master_cat = Path(out_dir, f"{ccd}.final.catalogue.parquet")
    # Create to_match_cat from df
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        df_file = tmp_file.name
    to_match_cat.to_parquet(df_file, index = False)
    # The command below should probably be merged with stilts_crossmatch_pair
    # But it requires some refactoring...
    matches = stilts_crossmatch_external(logger, Path(df_file), master_cat, ra_str, dec_str, match_radius)
    # Delete temp file
    Path(df_file).unlink()
    matches.dropna(subset=["Separation"])
    total_matched = len(matches)
    new_cols = matches.columns.to_list()
    # Default output is ['ID_1', 'Type', 'Subtype', 'RA_1', 'Dec_1', 'I', 'V', 'V_I', 'P_1',
    #    'REMARKS', 'RA_g', 'Dec_g', 'ID_g', 'nobs_g', 'RA_r', 'Dec_r', 'ID_r',
    #    'nobs_r', 'Separation_1', 'RA_i', 'Dec_i', 'ID_i', 'nobs_i',
    #    'Separation_1a', 'RA_z', 'Dec_z', 'ID_z', 'nobs_z', 'Separation_2',
    #    'RA_2', 'Dec_2', 'ID_2', 'Separation']
    id_col_label = "ID_2" if "ID_2" in new_cols else "ID"

    if total_matched > 0:
        logger.info(f"{total_matched} match(es) found. Creating lightcurves and cross-matched catalogue.")
        timestamp_iso8601 = datetime.now().isoformat().replace(':', '-')
        cat_path = Path(save_dir, f"{timestamp_iso8601}_result.csv")
        matches.to_csv(cat_path, index=False)
        logger.info(f"Catalogue created at {cat_path}")
        # Collect matches per band, then combine accordingly
        all_results = []
        for band in "griz":
            db_path = Path(out_dir, f"{ccd}.{band}.duckdb")
            id_col = matches[f"ID_{band}"]
            id_mask = id_col.notna()
            valid_ids = id_col[id_mask].values
            main_ids = matches[id_col_label][id_mask].values
            stack_df = get_rows_by_ids(db_path, f"lightcurves_{band}",valid_ids)
            stack_df["band"] = band
            stack_df.rename(columns={"ID" : "ID_band"}, inplace=True)
            id_dict = dict(zip(valid_ids.astype(int),main_ids.astype(int)))
            stack_df['ID_band'] = stack_df['ID_band'].astype(float).astype(int)
            stack_df["ID"] = stack_df['ID_band'].map(id_dict)
            all_results.append(stack_df)

        concat_df = pd.concat(all_results)
        # For simplicity store as single file. Later add option to store individual files
        concat_df.to_csv(Path(save_dir, f"{ccd}.query_results.csv"), index = False)
    else:
        logger.info("No matches found. No curves have been extracted.")
    