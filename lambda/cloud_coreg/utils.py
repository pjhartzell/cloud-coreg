import json
import logging
import os
import re
from dataclasses import dataclass
from distutils.util import strtobool
from pathlib import Path
from typing import Any, Dict, Optional

import boto3

logger = logging.getLogger()
s3_client = boto3.client("s3")


@dataclass
class CodemParameters:
    "Parses CODEM parameters from S3 bucket uploads and API POSTs"
    aoi_bucket: str
    aoi_file: str
    fnd_bucket: Optional[str] = None
    fnd_file: Optional[str] = None
    fnd_buffer_factor: float = float(os.environ["BUFFER_FACTOR"])
    min_resolution: float = float("nan")
    solve_scale: bool = bool(strtobool(os.environ["SOLVE_SCALE"]))
    email: Optional[str] = None

    @classmethod
    def from_bucket_upload(cls, message: Dict[str, Any]) -> "CodemParameters":
        return CodemParameters(
            aoi_bucket=message["s3"]["bucket"]["name"],
            aoi_file=message["s3"]["object"]["key"],
        )

    @classmethod
    def from_api_post(cls, message: Dict[str, Any]) -> "CodemParameters":
        if message.get("codemSolveScale", None) is not None:
            solve_scale = message["codemSolveScale"]
        else:
            solve_scale = cls.solve_scale

        return CodemParameters(
            aoi_bucket=os.environ["API_AOI_BUCKET"],
            aoi_file=message["aoiFile"],
            fnd_bucket=os.environ["API_FND_BUCKET"],
            fnd_file=message.get("fndFile", None),
            fnd_buffer_factor=message.get("fndBufferFactor", None)
            or cls.fnd_buffer_factor,
            min_resolution=message.get("codemMinResolution", None)
            or cls.min_resolution,
            solve_scale=solve_scale,
            email=message.get("email", None),
        )


def parse_message(message: Dict[str, Any]) -> CodemParameters:
    # Assumes sqs messages contain a single 'Record'
    body = json.loads(message["Records"][0]["body"])

    if body.get("Records", None):
        # Assumes a single bucket event record
        return CodemParameters.from_bucket_upload(body["Records"][0])
    else:
        return CodemParameters.from_api_post(body)


def download_file_s3(
    bucket: Optional[str] = None, file: Optional[str] = None
) -> Optional[str]:
    if bucket is None or file is None:
        return None
    download_path = f"/tmp/codem/downloads/{file}"
    s3_client.download_file(bucket, file, download_path)
    logger.info(f"S3 file downloaded: {download_path}")
    return download_path


def upload_directory_s3(bucket: str, local_directory: str, s3_directory: str) -> None:
    for file in Path(local_directory).glob("*"):
        s3_upload_path = f"{s3_directory}/{file.name}"
        s3_client.upload_file(
            str(file),
            bucket,
            s3_upload_path,
        )
        logger.info(f"Uploaded file to s3: {s3_upload_path}")


def extract_date(string: str) -> str:
    date_time = re.findall(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})", string)
    if not date_time or len(date_time) > 1:
        raise ValueError(f"Unexpected registration directory name: {string}")
    return date_time[0]


def create_directories() -> None:
    dirs = ["foundation", "foundation/3dep", "downloads"]
    for dir in dirs:
        Path(f"/tmp/codem/{dir}/").mkdir(parents=True)
