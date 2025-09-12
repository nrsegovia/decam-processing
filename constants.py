'''
Define Constants
'''

# Directories containing output of photpipe. 

ALL_FIELDS = {# DECaPS East Field (DDT)
              "EAST_SHOCD" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_shocd/",
              "EAST_ONE" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_regular/",
              "EAST_TWO" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_regular2/",
              "EAST_072824" : "/data/DECAMNOAO/ShoCd/workspace/decaps_east_072824/", #DDMMYY
              # DECaPS West Field(DDT)
              "WEST_ONE" : "/data/DECAMNOAO/ShoCd/workspace/decaps_west1/",
              "WEST_TWO" : "/data/DECAMNOAO/ShoCd/workspace/decaps_west/",
              # Eta Carinae Field, PI Catelan only
              "ETA_GOOD" : "/data/DECAMNOAO/ShoCd/workspace/eta_carinae_gp/",
              "ETA_BAD" : "/data/DECAMNOAO/ShoCd/workspace/eta_carinae_bp/", # Bad as in unwanted offset in pointing
              # B1 Field (DDT)
              "B1" : "/data/DECAMNOAO/ShoCd/workspace/b1/", # Note: incomplete processing
              }

# Mode names and definition
MODES = {"HDF_TO_PARQUET" : "Convert HDF files in a directory to parquet. Old files are removed.",
         "DCMP_TO_PARQUET" : "Convert dcmp files in a directory created by photpipe to parquet. New subdirectories per each requested band are created."}
MODES_STRING = "\n".join([f"{x} = {MODES[x]}" for x in MODES.keys()])