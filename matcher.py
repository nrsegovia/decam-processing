import logging
from pathlib import Path
import tempfile
from constants import *
import subprocess
import pandas as pd
import numpy as np
from utils import *
import concurrent.futures

# Call topcat/stilts, no multiprocessing customization as I do not know how stilts scales.

def compute_n_cols(type_col: pd.Series):

    vals = type_col.values
    eq1 = vals == 1
    eq3 = vals == 3
    na = np.isnan(vals)
    n1 = np.where(eq1, 1, 0)
    n3 = np.where(eq3, 1, 0)
    nplus = np.where((~np.logical_or(eq1,eq3)) | na, 1, 0)

    return [n1, n3, nplus]

def post_process_first_crossmatch(logger, df: pd.DataFrame, zpt_one, zpt_two) -> pd.DataFrame:
    """Post-process the first crossmatch between two catalogs."""
    logger.info("Post-processing first crossmatch...")
    
    cols_to_avg = PROCESSING['columns_to_average']
    error_cols = PROCESSING['error_columns']
    type_col = PROCESSING['type_column']
    
    result_data = {}
    
    # Process type counts based on type_1 and type_2 columns
    logger.debug("Computing initial type counts and adding zeropoints...")
    
    df["M_1"] = df["M_1"] + zpt_one
    df["M_2"] = df["M_2"] + zpt_two

    type_col_1 = f"{type_col}_1"
    type_col_2 = f"{type_col}_2"

    n1_1, n3_1, nplus_1 = compute_n_cols(df[type_col_1])
    n1_2, n3_2, nplus_2 = compute_n_cols(df[type_col_2])

    result_data['n1'] = n1_1 + n1_2
    result_data['n3'] = n3_1 + n3_2
    result_data['n+'] = nplus_1 + nplus_2
    
    # Compute n_total
    result_data['n_total'] = result_data['n1'] + result_data['n3'] + result_data['n+']
    
    logger.debug(f"Initial counts - n1: {result_data['n1'].sum()}, n3: {result_data['n3'].sum()}, "
                f"n+: {result_data['n+'].sum()}, n_total: {result_data['n_total'].sum()}")
    
    # Process separation (for first crossmatch, it's just the STILTS separation)
    result_data['Separation'] = df.Separation.fillna(0.0)
    
    # Process averaged columns (simple average for first crossmatch)
    for col in cols_to_avg:
        col1 = f"{col}_1"
        col2 = f"{col}_2"
        vals1 = df[col1].values
        vals2 = df[col2].values
        result_data[col] = np.nanmean([vals1, vals2], axis = 0)
    
    # Process error columns (simple error propagation for average)
    for col in error_cols:
        col1 = f"{col}_1"
        col2 = f"{col}_2"
        err1 = df[col1].fillna(0)
        err2 = df[col2].fillna(0)
        
        # For simple average: σ = √(σ₁² + σ₂²) / 2
        result_data[col] = np.sqrt(err1**2 + err2**2) / 2

    # Initialize magnitude range tracking
    if 'M' in cols_to_avg:
        m1 = df.get('M_1', pd.Series(dtype=float))
        m2 = df.get('M_2', pd.Series(dtype=float))
        
        result_data['M_min'] = np.full(len(df), np.nan)
        result_data['M_max'] = np.full(len(df), np.nan)
        result_data['M_range'] = np.full(len(df), np.nan)
        
        for i in range(len(df)):
            mag_vals = []
            if i < len(m1) and pd.notna(m1.iloc[i]):
                mag_vals.append(m1.iloc[i])
            if i < len(m2) and pd.notna(m2.iloc[i]):
                mag_vals.append(m2.iloc[i])
            
            if mag_vals:
                result_data['M_min'][i] = np.min(mag_vals)
                result_data['M_max'][i] = np.max(mag_vals)
                result_data['M_range'][i] = result_data['M_max'][i] - result_data['M_min'][i]
    
    result_df = pd.DataFrame(result_data)
    
    logger.info(f"First crossmatch post-processing completed: {len(result_df)} rows")
    logger.info(f"Source counts - n1: {result_df['n1'].sum()}, n3: {result_df['n3'].sum()}, "
            f"n+: {result_df['n+'].sum()}")
    
    return result_df

def post_process_subsequent_crossmatch(logger, df: pd.DataFrame, new_zpt) -> pd.DataFrame:
    """Post-process subsequent crossmatches with accumulated results."""
    logger.info("Post-processing subsequent crossmatch...")
    
    cols_to_avg = PROCESSING['columns_to_average']
    error_cols = PROCESSING['error_columns']
    type_col = PROCESSING['type_column']
    
    result_data = {}
    
    # Process type counts - add new contributions to existing counts
    logger.debug("Updating type counts and including zeropoint...")
    # Special case: magnitudes from "new" catalogue need zeropoint
    df["M_2"] = df["M_2"] + new_zpt
    # Get existing counts from catalog 1 (accumulated results)
    existing_n1 = df.get('n1', pd.Series(0, index=df.index)).fillna(0).astype(int)
    existing_n3 = df.get('n3', pd.Series(0, index=df.index)).fillna(0).astype(int)
    existing_nplus = df.get('n+', pd.Series(0, index=df.index)).fillna(0).astype(int)
    
    
    n1, n3, nplus = compute_n_cols(df[type_col])

    result_data['n1'] = n1 + existing_n1
    result_data['n3'] = n3 + existing_n3
    result_data['n+'] = nplus + existing_nplus
    
    # Compute n_total
    result_data['n_total'] = result_data['n1'] + result_data['n3'] + result_data['n+']
    
    logger.debug(f"Updated counts - n1: {result_data['n1'].sum()}, n3: {result_data['n3'].sum()}, "
                f"n+: {result_data['n+'].sum()}, n_total: {result_data['n_total'].sum()}")
    
    # Process separation with proper averaging
    logger.debug("Computing updated separations...")
    result_data['Separation'] = np.zeros(len(df), dtype=float)
    prev_sep = df.Separation_1.fillna(0.0)
    new_sep = df.Separation.fillna(0.0)
    
    for i in range(len(df)):
        n_total = result_data['n_total'][i]
        
        if n_total <= 1:
            pass
        elif n_total == 2:
            result_data['Separation'][i] = new_sep.iloc[i]
        else:
            # Formula: (prev_sep * (n_total - 2) + new_sep) / (n_total - 1)
            result_data['Separation'][i] = (prev_sep.iloc[i] * (n_total - 2) + new_sep.iloc[i]) / (n_total - 1)
    
    # Process averaged columns with proper weighting
    for col in cols_to_avg:
        col1 = f"{col}_1"
        col2 = f"{col}_2"
        
        if col1 in df.columns and col2 in df.columns:
            # Get n_total values for weighting
            n_total_1 = df.n_total
            n_total_2 = pd.Series(1, index=df.index)  # New catalog contributes 1 source
            
            val1 = df[col1]
            val2 = df[col2]
            
            mask1 = pd.notna(val1)
            mask2 = pd.notna(val2)
            
            result_col = np.full(len(df), np.nan)
            
            # Both values present - weighted average
            both_mask = mask1 & mask2
            if both_mask.any():
                total_weight = n_total_1[both_mask] + n_total_2[both_mask]
                result_col[both_mask] = (
                    (val1[both_mask] * n_total_1[both_mask] + 
                    val2[both_mask] * n_total_2[both_mask]) / 
                    total_weight
                )
            
            # Only first value present
            only1_mask = mask1 & ~mask2
            if only1_mask.any():
                result_col[only1_mask] = val1[only1_mask]
            
            # Only second value present
            only2_mask = ~mask1 & mask2
            if only2_mask.any():
                result_col[only2_mask] = val2[only2_mask]
            
            result_data[col] = result_col
            
        elif col1 in df.columns:
            result_data[col] = df[col1].copy()
        elif col2 in df.columns:
            result_data[col] = df[col2].copy()
    
    # Process error columns with proper propagation
    for col in error_cols:
        col1 = f"{col}_1"
        col2 = f"{col}_2"
        
        if col1 in df.columns and col2 in df.columns:
            n_total_1 = df.n_total
            n_total_2 = pd.Series(1, index=df.index)
            
            err1 = df[col1].fillna(0)
            err2 = df[col2].fillna(0)
            
            total_weight = n_total_1 + n_total_2
            weight1 = n_total_1 / total_weight
            weight2 = n_total_2 / total_weight
            
            result_data[col] = np.sqrt((weight1 * err1)**2 + (weight2 * err2)**2)
        elif col1 in df.columns:
            result_data[col] = df[col1].copy()
        elif col2 in df.columns:
            result_data[col] = df[col2].copy()
    
    # Update magnitude range tracking
    if 'M' in cols_to_avg:
        m_min_1 = df.get('M_min_1', df.get('M_1', pd.Series(dtype=float)))
        m_max_1 = df.get('M_max_1', df.get('M_1', pd.Series(dtype=float)))
        m2 = df.get('M_2', pd.Series(dtype=float))
        
        result_data['M_min'] = np.full(len(df), np.nan)
        result_data['M_max'] = np.full(len(df), np.nan)
        result_data['M_range'] = np.full(len(df), np.nan)
        
        for i in range(len(df)):
            all_vals = []
            
            if i < len(m_min_1) and pd.notna(m_min_1.iloc[i]):
                all_vals.append(m_min_1.iloc[i])
            if i < len(m_max_1) and pd.notna(m_max_1.iloc[i]):
                all_vals.append(m_max_1.iloc[i])
            if i < len(m2) and pd.notna(m2.iloc[i]):
                all_vals.append(m2.iloc[i])
            
            if all_vals:
                result_data['M_min'][i] = np.min(all_vals)
                result_data['M_max'][i] = np.max(all_vals)
                result_data['M_range'][i] = result_data['M_max'][i] - result_data['M_min'][i]
    
    result_df = pd.DataFrame(result_data)
    
    logger.info(f"Subsequent crossmatch post-processing completed: {len(result_df)} rows")
    
    return result_df

def stilts_crossmatch_pair(logger,  catalog1_path: Path, catalog2_path: Path) -> pd.DataFrame:
        """Run STILTS crossmatch between two catalogs."""
        logger.info(f"Crossmatching {catalog1_path.name} with {catalog2_path.name}")
        
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

def match_list_of_files(logger, paths, idx):
    # Flag to track if this is the first crossmatch
    first_crossmatch = True
    current_result = None
    
    # Save current result as temporary file for STILTS
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        current_temp_file = tmp_file.name
    
    try:
        # Iteratively crossmatch files
        for i in range(len(paths) - 1):
            if first_crossmatch:
                # First crossmatch: match file[0] with file[1]
                file_path_1 = paths[i]
                file_path_2 = paths[i + 1]
                
                logger.info(f"First crossmatch: {file_path_1.stem} with {file_path_2.stem}")
                
                # Crossmatch the two files directly
                crossmatch_df = stilts_crossmatch_pair(logger, file_path_1, file_path_2)
                # Get required zeropoints
                zpt_one = get_from_header(file_path_1, "ZPTMAG")
                zpt_two = get_from_header(file_path_2, "ZPTMAG")
                # Post-process for first crossmatch
                current_result = post_process_first_crossmatch(logger, crossmatch_df, zpt_one, zpt_two)
                
                first_crossmatch = False
                logger.info(f"First crossmatch completed: {len(current_result)} rows")
                
            else:
                # Subsequent crossmatches: match current_result with next file
                file_path = paths[i + 1]
                
                logger.info(f"Subsequent crossmatch with: {file_path.stem}")
                
                # Save current result to temporary file
                current_result.to_parquet(current_temp_file, index=False)
                
                # Crossmatch current result with next file
                crossmatch_df = stilts_crossmatch_pair(logger, Path(current_temp_file), file_path)
                new_zpt = get_from_header(file_path, "ZPTMAG")

                # Post-process subsequent crossmatch
                current_result = post_process_subsequent_crossmatch(logger, crossmatch_df, new_zpt)
                
                logger.info(f"Crossmatch {i} completed: {len(current_result)} rows.")
        
        # Clean up columns we don't want in the final output
        final_columns = ['RA', 'Dec', 'M', 'dM', 'M_range', 'n1', 'n3', 'n+', 'n_total', 'Separation']
        available_columns = [col for col in final_columns if col in current_result.columns]
        current_result = current_result[available_columns]
        
    finally:
        try:
            Path(current_temp_file).unlink()
            return current_result, idx
        except:
            return None, None

def create_ccd_band_master_catalog(logger, field_path, ccd, bands):
    subdirs = [Path(field_path, ccd, band) for band in bands]
    problems = [not(x.is_dir()) for x in subdirs]
    any_problem = any(problems)
    if any_problem:
        logger.error(f"Problem with directories, check them.")
    else:
        try:
            to_match = [[x for x in y.glob("*.parquet")] for y in subdirs]
            logger.info(f"Found {[len(x) for x in to_match]} files {[b for b in bands]} to process.")
            # One worker per band max
            with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
                # Submit all tasks
                futures = [executor.submit(match_list_of_files, logger, file_list, idx) for idx, file_list in enumerate(to_match)]
                # Process results as they complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result_df, out_idx = future.result()
                        subdir = subdirs[out_idx]
                        if result_df is not None and len(result_df) > 0:
                            # Save results
                            output_file = Path(field_path, ccd, f"{ccd}.{bands[out_idx]}.catalogue.parquet")
                            result_df.to_parquet(output_file, index = False)
                            logger.info(f"Saved results for {subdir.name}: {len(result_df)} sources")
                        else:
                            logger.warning(f"No results generated for {subdir.name}")
                    except Exception as e:
                        logger.error(f"Found a problem: {e}.")
        except Exception as e:
            logger.error(f"Failed to process directory {subdir.name}: {e}")


def create_ccd_master_catalog(logger, field_path, ccd, bands = "griz"):

    for band in bands:
        create_ccd_band_master_catalog(logger, field_path, ccd, band)
    # Once done, load created catalogs per band and match them.