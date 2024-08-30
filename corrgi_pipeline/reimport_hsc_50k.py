from pathlib import Path

from dask.distributed import Client
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.pipeline import pipeline_with_client

from hsc_fits_reader import HSCFitsReader


def reimport_psc_threshold_50k():
    # Passband to import
    PASSBAND = "r"

    # Path to input FITS files
    RAW_CATALOG_PATH = (
        "/ocean/projects/phy210048p/shared/hipscat/raw/hsc/pdr3/catalog-forced/"
    )

    # Output path
    HIPSCAT_PATH = "/ocean/projects/phy210048p/shared/hipscat/test_catalogs"
    CATALOG_NAME = f"hsc-pdr3-forced-{PASSBAND}-50k"

    # Column specification
    RA_COLUMN = "coord_ra"
    DEC_COLUMN = "coord_dec"

    # Get input files
    input_file_list = sorted(
        Path(RAW_CATALOG_PATH).glob(f"forced_src-HSC-{PASSBAND.upper()}*.fits")
    )
    print(f"Found {len(input_file_list)} files")

    # Build arguments
    args = dict(
        sort_columns="id",
        ra_column=RA_COLUMN,
        dec_column=DEC_COLUMN,
        input_file_list=input_file_list,
        file_reader=HSCFitsReader(
            ra_column=RA_COLUMN,
            dec_column=DEC_COLUMN,
            flags_column="flags",
            # we cannot do flags yet, see
            # https://github.com/astronomy-commons/hipscat-import/issues/351
            skip_column_names=["flags"],
        ),
        lowest_healpix_order=3,  # It would be something between 3 and 6
        pixel_threshold=50_000,  # very small num of rows per partition
        drop_empty_siblings=True,
        output_path=HIPSCAT_PATH,
        output_artifact_name=CATALOG_NAME,
        simple_progress_bar=True,
        resume=True,
    )

    # For the first stages we need 8GB/worker, for the last stage - 64GB
    with Client(n_workers=32) as client:
        args1 = ImportArguments(
            **args,
            run_stages=["planning", "mapping", "binning", "splitting"],
        )
        pipeline_with_client(args1, client)

    with Client(n_workers=4) as client:
        args2 = ImportArguments(
            **args,
            run_stages=["reducing", "finishing"],
        )
        pipeline_with_client(args2, client)


if __name__ == "__main__":
    reimport_psc_threshold_50k()
