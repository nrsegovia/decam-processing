'''
Define Constants
'''

# Directories containing output of photpipe. 

ALL_FIELDS = {# DECaPS East Field (DDT)
              "EAST_SHOCD" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_shocd/",
              "EAST_ONE" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_regular/",
              "EAST_TWO" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_regular2/",
              "EAST_240728" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_240728/", #YYDDMM
              # DECaPS West Field(DDT)
              "WEST_ONE" : "/data/DECAMNOAO/ShoCd/workspace/decaps_west1/",
              "WEST_TWO" : "/data/DECAMNOAO/ShoCd/workspace/decaps_west/",
              # Eta Carinae Field, PI Catelan only
              "ETA_GOOD" : "/data/DECAMNOAO/ShoCd/workspace/eta_carinae_gp/",
              "ETA_BAD" : "/data/DECAMNOAO/ShoCd/workspace/eta_carinae_bp/", # Bad as in unwanted offset in pointing
              # B1 Field (DDT)
              "B1" : "/data/DECAMNOAO/ShoCd/workspace/b1/", # Note: incomplete processing
              }

# STILTS or TOPCAT path. Must be the parquet-compatible version, usually topcat-extra.jar

STILTS = "/data/DECAMNOAO/ShoCd/code/src/topcat-extra.jar"

# Mode names and definition
MODES = {"HDF_TO_PARQUET" : "Convert HDF files in a directory to parquet. Old files are removed.",
         "DCMP_TO_PARQUET" : "Convert dcmp files in a directory created by photpipe to parquet. New subdirectories per each requested band are created.",
         "CATALOG_PER_CCD_BAND" : "Creates catalog of all sources observed at least once as type 1 or 3 in a given configuration of field-ccd-band(s). Uses STILTS, so the number of workers set here is irrelevant."}
MODES_STRING = "\n".join([f"{x} = {MODES[x]}" for x in MODES.keys()])

# Crossmtach (lightcurve) creation configuration.
CROSSMATCH = {"radius"    : 1.0,
              "col1_ra"   : "RA",
              "col1_dec"  : "Dec",
              "col2_ra"   : "RA",
              "col2_dec"  : "Dec",
              "join_type" : "1or2"}

PROCESSING = {"input_columns"      : ["RA", "Dec", "M", "dM", "flux", "dflux", "type"],
              "columns_to_average" : ["RA", "Dec", "M"],
              "error_columns"      : ["dM"],
              "columns_to_drop"    : ["flux", "dflux"],
              "type_column"        : "type"} 