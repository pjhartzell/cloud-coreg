import dataclasses
import logging

import codem

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def run_codem(foundation, aoi, parameters):
    codem_run_config = codem.CodemRunConfig(
        FND_FILE=foundation,
        AOI_FILE=aoi,
        MIN_RESOLUTION=parameters.min_resolution,
        DSM_SOLVE_SCALE=parameters.solve_scale,
        ICP_SOLVE_SCALE=parameters.solve_scale,
    )
    config = dataclasses.asdict(codem_run_config)
    fnd_obj, aoi_obj = codem.preprocess(config)
    fnd_obj.prep()
    aoi_obj.prep()
    dsm_reg = codem.coarse_registration(fnd_obj, aoi_obj, config)
    icp_reg = codem.fine_registration(fnd_obj, aoi_obj, dsm_reg, config)
    reg_file = codem.apply_registration(fnd_obj, aoi_obj, icp_reg, config)
    logger.info("Registered AOI to Foundation")
    return reg_file
