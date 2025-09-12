from pathlib import Path

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