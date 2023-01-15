import logging
import os
import shutil
from pathlib import Path

from .coregister import run_codem
from .foundation import generate_foundation
from .utils import (
    create_directories,
    download_file_s3,
    extract_date,
    parse_message,
    upload_directory_s3,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(f"event message: \n{event}")
    parameters = parse_message(event)
    create_directories()

    aoi_path = download_file_s3(parameters.aoi_bucket, parameters.aoi_file)

    fnd_path = download_file_s3(
        parameters.fnd_bucket, parameters.fnd_file
    ) or generate_foundation(aoi_path, parameters.fnd_buffer_factor)

    registered_aoi_path = run_codem(fnd_path, aoi_path, parameters)

    upload_directory_s3(
        os.environ.get("RESULT_BUCKET"),
        str(Path(registered_aoi_path).parent),
        f"{Path(aoi_path).stem}-registered-{extract_date(str(Path(registered_aoi_path).parent))}",
    )

    # TODO: send email

    shutil.rmtree("/tmp/codem/")
    logger.info("Registration complete.")
