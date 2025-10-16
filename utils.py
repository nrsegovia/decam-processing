from pathlib import Path
import pandas as pd
from collections import Counter, defaultdict

def in_ccd_range(value: int):
    return (value > 0) & (value < 62)

def replace_string_in_file(path: Path, string_to_replace: str, replacement: str):
    with open(path, "r") as f:
        contents = f.read()
    
    update = contents.replace(string_to_replace, replacement)

    with open(path, "w") as f:
        f.write(update)

def list_directories(main_dir, ccds, single_ccd, bands, single_band):
    directories = []
    str_ccds = str(ccds) if single_ccd else [str(x) for x in ccds]

    # Single CCD and single band
    if single_ccd and single_band:
        directories.append(Path(main_dir, str_ccds, bands))
    # Single CCD and multiple bands
    elif single_ccd:
        for band in bands:
            directories.append(Path(main_dir, str_ccds, band))
    # Single band and multiple CCDs
    elif single_band:
        for ccd in str_ccds:
            directories.append(Path(main_dir, ccd, bands))
    # Multiple CCDs and multiple bands
    else:
        for ccd in str_ccds:
            for band in bands:
                directories.append(Path(main_dir, ccd, band))
    
    return(directories)

def list_directories_no_band(main_dir, ccds, single_ccd):
    directories = []
    str_ccds = str(ccds) if single_ccd else [str(x) for x in ccds]

    # Single CCD
    if single_ccd:
        directories.append(Path(main_dir, str_ccds))
    # Multiple CCDs
    else:
        for ccd in str_ccds:
            directories.append(Path(main_dir, ccd))
    
    return(directories, str_ccds)

def get_from_header(file_path: Path, column: str):
    header = pd.read_csv(Path(file_path.parent, "Header.info"))
    row = header.File[header.File.values == file_path.name].index.to_list()[0]
    try:
        value = header.at[row, column]
    except Exception as e:
        value = None
    return value

def check_points_in_rectangle(df, x_coord, y_coord, x_min, x_max, y_min, y_max):
    """Check if points in DataFrame are inside rectangle"""
    return (
        (df[x_coord] >= x_min) & (df[x_coord] <= x_max) &
        (df[y_coord] >= y_min) & (df[y_coord] <= y_max)
    )

def get_file_priority(file_path, dir_priority):
    """
    Get priority of a file based on which priority directory it's under.
    Returns (priority_index, distance_from_priority_dir)
    Lower values = higher priority
    """
    file_path = Path(file_path).resolve()
    
    # Check if file is under any priority directory
    for priority_dir, priority_idx in dir_priority.items():
        try:
            # Check if file is relative to this priority dir
            relative = file_path.relative_to(priority_dir)
            # Return priority index and depth (for tie-breaking)
            return (priority_idx, len(relative.parts))
        except ValueError:
            # Not under this directory
            continue
    
    # If not under any priority directory, lowest priority
    return (float('inf'), float('inf'))

def check_and_handle_mjd_duplicates(logger, path_list, priority_dirs):
    """
    Check for MJD duplicates and rename lower-priority files.
    
    Args:
        logger: Logger instance
        path_list: List of Path objects to parquet files
        priority_dirs: List of directory paths in priority order (highest first)
                      e.g., ["/data/dir1/", "/data/dir2/", "/data/dir3/"]
    
    Returns:
        list: Filtered path_list with only highest-priority files (duplicates renamed)
    """
    # Extract MJD for each file
    mjds = []
    for path in path_list:
        try:
            mjd = get_from_header(path, "MJD-OBS")
            mjds.append((path, mjd))
        except Exception as e:
            logger.warning(f"Could not read MJD from {path}: {e}")
            continue
    
    mjd_values = [m[1] for m in mjds]
    unique_mjds = set(mjd_values)
    
    # Check for duplicates
    if len(mjd_values) == len(unique_mjds):
        logger.info("No duplicate MJD values found")
        return None
    
    logger.warning(f"Found {len(mjd_values) - len(unique_mjds)} duplicate MJD values")
    
    # Group files by MJD
    mjd_groups = defaultdict(list)
    for path, mjd in mjds:
        mjd_groups[mjd].append(path)
    
    # Create priority mapping for directories
    # Lower index = higher priority
    dir_priority = {Path(d).resolve(): idx for idx, d in enumerate(priority_dirs)}
    
    # Process each MJD group
    files_to_keep = []
    renamed_count = 0
    
    for mjd, files in mjd_groups.items():
        if len(files) == 1:
            # No duplicates for this MJD
            files_to_keep.append(files[0])
        else:
            # Found duplicates - determine which to keep
            logger.info(f"MJD {mjd}: {len(files)} duplicate files")
            
            # Sort by priority (lowest priority value = highest priority)
            files_with_priority = [(f, get_file_priority(f, dir_priority)) for f in files]
            files_with_priority.sort(key=lambda x: x[1])
            
            # Keep the highest priority file
            keep_file, keep_priority = files_with_priority[0]
            files_to_keep.append(keep_file)
            
            # Find which priority directory the kept file belongs to
            keep_priority_idx = keep_priority[0]
            if keep_priority_idx < len(priority_dirs):
                keep_dir = priority_dirs[keep_priority_idx]
                logger.info(f"  Keeping: {keep_file} (priority dir: {keep_dir})")
            else:
                logger.info(f"  Keeping: {keep_file} (not in priority dirs)")
            
            # Rename the rest
            for file_path, file_priority in files_with_priority[1:]:
                # Create new name with _duplicate suffix
                file_path = Path(file_path)
                stem = file_path.stem
                suffix = file_path.suffix
                new_name = f"{stem}_duplicate{suffix}"
                new_path = file_path.parent / new_name
                
                # Rename the file
                try:
                    # file_path.rename(new_path)
                    renamed_count += 1
                    
                    # Log which priority dir it belonged to
                    file_priority_idx = file_priority[0]
                    if file_priority_idx < len(priority_dirs):
                        orig_dir = priority_dirs[file_priority_idx]
                        logger.info(f"  Renamed: {file_path.name} → {new_name} "
                                  f"(priority dir: {orig_dir})")
                    else:
                        logger.info(f"  Renamed: {file_path.name} → {new_name} "
                                  f"(not in priority dirs)")
                except Exception as e:
                    logger.error(f"  Failed to rename {file_path}: {e}")
    
    logger.info(f"Duplicate handling complete: {renamed_count} files renamed, "
               f"{len(files_to_keep)} files kept")