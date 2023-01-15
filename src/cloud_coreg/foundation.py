import logging
from collections import Counter
from pathlib import Path
from typing import List

import planetary_computer as pc
import pystac_client
import rasterio
import rasterio.mask
import rasterio.merge
import requests
from codem import resources
from rasterio.warp import (
    Resampling,
    calculate_default_transform,
    reproject,
    transform_bounds,
)
from shapely.affinity import scale
from shapely.geometry import box, mapping

STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"

logger = logging.getLogger()


def generate_foundation(aoi_path: str, buffer_factor: float) -> str:
    foundation_bbox = get_foundation_bbox(aoi_path, buffer_factor)

    catalog = pystac_client.Client.open(STAC_API)
    query = catalog.search(collections=["3dep-lidar-dsm"], bbox=foundation_bbox)
    items = list(query.items())
    dsm_urls = [i.assets["data"].href for i in items]
    if len(dsm_urls) > 0:
        logger.info(
            f"Planetary Computer USGS 3DEP query returned {len(dsm_urls)} items."
        )
    else:
        raise ValueError("No USGS 3DEP foundation data found")

    dsm_paths = download_dsms(dsm_urls, "/tmp/codem/foundation/3dep/")
    logger.info("Foundation DSMs downloaded")

    if len(dsm_paths) > 1:
        consistent_dsm_paths = make_consistent(dsm_paths)
        merged_dsm_path = merge_dsms(consistent_dsm_paths, "/tmp/codem/foundation/")
        logger.info("Foundation DSMs merged.")
    else:
        merged_dsm_path = dsm_paths[0]

    cropped_dsm_path = crop_dsm(
        merged_dsm_path, foundation_bbox, "/tmp/codem/foundation/"
    )
    logger.info("Foundation DSM cropped to foundation bounding box.")

    return cropped_dsm_path


def crop_dsm(path: str, bbox: List[float], destination: str) -> str:
    with rasterio.open(path) as src:
        bbox_src_crs = rasterio.CRS.from_epsg(4326)
        bbox_dst_crs = src.crs
        projected_box = mapping(
            box(*transform_bounds(bbox_src_crs, bbox_dst_crs, *bbox))
        )
        cropped_image, cropped_transform = rasterio.mask.mask(
            src, [projected_box], crop=True
        )
        cropped_meta = src.meta

    cropped_meta.update(
        {
            "driver": "GTiff",
            "height": cropped_image.shape[1],
            "width": cropped_image.shape[2],
            "transform": cropped_transform,
        }
    )

    cropped_path = Path(destination, "cropped.tif").as_posix()
    with rasterio.open(cropped_path, "w", **cropped_meta) as dst:
        dst.write(cropped_image)

    return cropped_path


def merge_dsms(dsm_paths: List[str], destination: str) -> str:
    open_dsms = [rasterio.open(dsm_path) for dsm_path in dsm_paths]
    merged, transform = rasterio.merge.merge(open_dsms)

    out_meta = open_dsms[0].meta.copy()
    for open_dsm in open_dsms:
        open_dsm.close()
    out_meta.update(
        {
            "driver": "GTiff",
            "height": merged.shape[1],
            "width": merged.shape[2],
            "transform": transform,
        }
    )

    merged_path = Path(destination, "merged.tif")
    with rasterio.open(merged_path, "w", **out_meta) as dst:
        dst.write(merged)

    return str(merged_path)


def make_consistent(dsm_paths: List[str]) -> List[str]:
    """Handles differing projections, but not differing spatial resolutions"""
    crss = []
    for dsm_path in dsm_paths:
        with rasterio.open(dsm_path) as src:
            crss.append(src.crs)

    crs_count = Counter(crss)
    dst_crs = crs_count.most_common(1)[0][0]

    consistent_dsm_paths = []
    for dsm_path in dsm_paths:
        with rasterio.open(dsm_path) as src:
            if src.crs != dst_crs:
                consistent_path = str(
                    Path(dsm_path).parent / f"{Path(dsm_path).stem}_reprojected.tif"
                )
                transform, width, height = calculate_default_transform(
                    src.crs, dst_crs, src.width, src.height, *src.bounds
                )
                kwargs = src.meta.copy()
                kwargs.update(
                    {
                        "crs": dst_crs,
                        "transform": transform,
                        "width": width,
                        "height": height,
                    }
                )
                with rasterio.open(consistent_path, "w", **kwargs) as dst:
                    reproject(
                        source=rasterio.band(src, 1),
                        destination=rasterio.band(dst, 1),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest,
                    )
                consistent_dsm_paths.append(consistent_path)
            else:
                consistent_dsm_paths.append(dsm_path)
    return consistent_dsm_paths


def download_dsms(urls: List[str], destination: str) -> List[str]:
    local_paths = []
    for url in urls:
        download_path = str(Path(destination, Path(url).name))
        signed_url = pc.sign(url)
        response = requests.get(signed_url, stream=True)
        with open(download_path, "wb") as fout:
            for chunk in response.iter_content(chunk_size=4096):
                fout.write(chunk)
        local_paths.append(download_path)
    return local_paths


def get_foundation_bbox(aoi_path: str, buffer_factor: float) -> List[float]:
    ext = Path(aoi_path).suffix

    if ext in resources.dsm_filetypes:
        with rasterio.open(aoi_path) as dsm:
            src_bounds = box(*dsm.bounds)
            src_crs = dsm.crs
        src_buffered = scale(src_bounds, xfact=buffer_factor, yfact=buffer_factor)
        wgs84_buffered = transform_bounds(
            src_crs, {"init": "EPSG:4326"}, *src_buffered.bounds
        )
        wgs84_bbox = list(wgs84_buffered)

    elif ext in resources.mesh_filetypes:
        raise ValueError(f"File type '{ext}' is not supported.")
        # https://trimsh.org/trimesh.parent.html?highlight=box#trimesh.parent.Geometry3D.bounding_box
        # https://trimsh.org/trimesh.parent.html?highlight=box#trimesh.parent.Geometry3D.bounding_box_oriented

    elif ext in resources.pcloud_filetypes:
        raise ValueError(f"File type '{ext}' is not supported.")

    else:
        raise ValueError(f"File type '{ext}' is not supported.")

    return wgs84_bbox
