# decam-processing

Tools for further processing of DECam data products after photpipe processing. Scripts here are meant to be used in the `esmeralda-mint2` server. The scripts and methods shared here have been developed by [Nicolas Rodriguez-Segovia](https://nrsegovia.github.io/), feel free to reach out and ask questions, report bugs or suggest improvements.


## Contents

- [Data description](#data-description)
- [Reducing NOIRLab files using `photpipe`](#reducing-noirlab-files-using-photpipe)
- [BEFORE USING THE SCRIPTS](#before-using-the-scripts)
- [Adding `photpipe` catalogues to the database](#adding-photpipe-catalogues-to-the-database)
- [Checking data coverage](#checking-data-coverage)
- [Retrieving data](#retrieving-data)
- [Exploring individual catalogues](#exploring-individual-catalogues)
- [Exploring individual images](#exploring-individual-images)

## Data description
## Reducing NOIRLab files using `photpipe`
## BEFORE USING THE SCRIPTS

The following sections make use of the scripts presented in this repository. As such, I assume that you are using them in the `\data\DECAMNOAO\ShoCd\decam-processing` directory. I also assume that you have created a conda environment using the `environment.yml` file and activated it:
```
conda env create -f environment.yml
conda activate decam
```
Or, at the very least, you have installed the python packages listed in `environment.yml`.

## Adding `photpipe` catalogues to the database

## Checking data coverage

You can run the `visualize_coverage.py` script to take a look at the current data available or, more specifically, the approximate CCD coverage for all the processed fields. An example is given below for the `EAST` field.

![DECaPS East field](EAST_footprint.png)

## Retrieving data

To fully take advantage of the database, the data (light curve) retrieval works in two steps. The first step matches a user-provided catalog against the master catalogues built in the previous section. The match makes uses of `STILTS` and its `tmatch2` algorithm, under the `find=all` configuration. This means that all matches within the user-provided radius and coordinates are returned. This results in a matched catalogue which you will find in the output directory.

The matched catalogue contains both the input catalogue columns as well as information from the master catalogue. The latter is internally used when retrieving data points from the database (second step of the process), and is necessary to identify individual light curves in the resulting data.

### Example light curve retrieval

As of writing this documentation, only `csv` format is allowed for input catalogues. In principle, only columns for right ascension and declination are required, though for posterior analysis ID and period or columns related to other relevant data are encouraged. For this example, I will name my catalogue `variables.csv` and it will contain the `ID,RA,Dec` columns. Then, matching is as simple as:

```
./decam_processing.py --directory EAST --mode LIGHTCURVE --inputcat /full/path/to/variables.csv --ccds 1 [--parquet] [--outdir /full/path/to/desired/output/location] [--radius 1.0] [--ra RA] [--dec Dec]
```
The arguments are briefly explained below, and you can also find some documentation by running `./decam_processing.py` without arguments. Also, arguments within brackets are optional.
- `directory` corresponds to the directory of data to be processed or analyzed. When retrieving light curves, only `EAST` is currently available.
- `mode` selects what you want to do. In this case, we build light curves, hence the name.
- `inputcat` is the input catalogue. As described above, only csv files are supported and you need columns for RA and Dec (in decimal degrees), at the very least.
- `ccds` corresponds to the ccds being used in the process. HOWEVER, this argument is NOT used in the `LIGHTCURVE` (current) mode. Due to an oversight on my part it is required for the command to succesfully run, though. Setting it as `--ccd 1` will be enough. 
- `parquet` sets whether your output will be `parquet`-formatted (if the flag is present) or `csv` (otherwise). The `parquet` file format is fast and lightweight, though not as easy to read nor modify as `csv`.
- `outdir` sets where to save the output to. It defaults to the local `output/lightcurves` directory, though using the default value is not recommended as your results might be overwritten by yourself or other users.
- `radius` sets the crossmatch radius for your input catalogue sources. The default is 1.0 arseconds, and the maximum value allowed is 10.0 arcseconds.
- `ra` sets the name of the right ascension column in your input catalogue. The default is RA.
- `dec` sets the name of the declination column in your input catalogue. The default is Dec.

If your input catalogue is matched against any sources in the database, you will get at least 2 files as a result of the process. The maximum number of files you get would be 122. This is because you get one matched catalogue plus one data file per CCD with matching data.

### Using the matched data

Since the results are not _per source_, you still have the task of separating them and checking for overlaps ahead of you. The following lines contain some advice/ideas, but you are free to choose whatever you want to do with the data.

- The resulting file contains a `Separation` column. It is directly related to the selected cross-match radius, as no `Separation` value should be over the imposed threshold. Trends in this separation can give you some clues, e.g., lower values mean matches closer to your input coordinates.
- Filtering by `dotype` is a good idea, as generally you will only be interested in stellar sources. This can be done by using only `type in [1,3]`, though for a complete description you can take a look at the `DoPhot` manual (for en excerpt, take a look at [the end of this documentation]())
- The way in which the database has been built implies that there are several steps in identifying a given source. The first one is its location within the CCD subsets: a given source may appear in more than a single CCD if it lies near a CCD edge. Hence, you could use the input `ID` column (as recommended before) to look for sources with matches in multiple CCDs.
- If you are not using an input `ID` column, you can still use the `GroupID` column as a way of grouping matches found for a given input coordinate. Beware that sources with a single match are not considered a group and therefore `GroupID` is empty (`NaN`). Then, for each `GroupID` value (or each empty row), you can extract data from the data results (file in the format `CCD.query_results.csv` or `parquet`) by looking for matching rows using the `ID` column. This can be done by using the fourth to last column in your matched catalogue (before `GroupID`) and the `ID` column of the data. I know that this sounds complicated so I have created an `example.ipynb` file.

## Exploring individual catalogues
## Exploring individual images
## Additional information
### DOtype
The following table has been retrieved from the manual uploaded to the [DoPHOT_C repository](https://github.com/M1TDoPHOT/DoPHOT_C/tree/master).

| DoType | Description |
| --- | --- |
|Type 1|A 'perfect' star. Was used in computing weighted mean shape parameters for stars.|
|Type 2|Object is not as peaked as a star, and has been interpreted to be a 'galaxy'. Shape parameters were calculated specifically for this object.|
|Type 3|An object found to be not as peaked as a single star was interpreted to be two very close stars. This is one component of such a close pair. Final fit to this component used mean shape parameters for single star.|
|Type 4|Failed to converge on a 4-parameter fit using mean single star shape. See section 7.9. Photometry is extremely unreliable.|
|Type 5|Not enough points with adequate S/N could be found around this object while attempting a 7-parameter fit to determine shape. Indicates a problem with the local environment, although a 4-parameter fit with mean star shape succeeded. Photometry is suspect, and this may not be a bona-fide star. See section 7.9.|
|Type 6|Too few points with adequate S/N to do even a 4-parameter fit with mean star shapes. Object NOT subtracted from image. See section 7.9.|
|Type 7|Object was too faint to attempt a 7-parameter fit. No object classification could be done, so it might be a faint galaxy or two closely spaced stars. Photometry is OK (from successful 4-parameter fit with mean shapes) provided the object is really a star. See section 7.4 for discussion of star/galaxy discrimination for such objects.|
|Type 8|These are obliterated regions due to data saturation or identification of cosmic rays. See Section 7.8. The shape descriptors are different for Type 8s than for other types: see Appendix B.|
|Type 9|If an attempt to determine the shape of an object fails to converge, it is classified as object type 9.  See section 7.9. Photometry is unreliable.|


- Convert `photpipe` output to `parquet` files
- Check for duplicates and remove (rename) as desired by using the mode `mode`
- Put everything into database by using the `mode` mode
