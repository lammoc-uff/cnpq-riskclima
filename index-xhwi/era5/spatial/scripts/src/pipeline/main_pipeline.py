from src.io.loaders import open_calibration, open_validation_pair
from src.utils.dask_client import start, stop
from src.pipeline.month_job import process_single_month
from src.config.settings import (
                                 RESULT_XHWI,
                                 RESULT_XHWI_DIARY_INDPROD,
                                 RESULT_XHWI_MONTH_INDPROD,
                                 )
from src.utils.logger import get_logger

logger = get_logger("main_pipeline")

def run_all_months(months=range(1, 13)):
    """
    Run the full heatwave index pipeline for all selected months.

    Parameters
    ----------
    months : iterable of int, optional
        List or range of month numbers (1–12) to process. Default is all months.
    """
    logger.info("🌡️ Starting full XHWI pipeline...")

    client = start()

    try:
        # 1️⃣ Load calibration and validation datasets
        logger.info("→ Opening calibration and validation datasets")
        calib_ds = open_calibration()
        val_ds = open_validation_pair()

        # 2️⃣ Iterate over months
        for m in months:
            logger.info(f"📅 Processing month {m}")
            process_single_month(
                                 month_idx=m,
                                 calib_ds=calib_ds,
                                 val_ds=val_ds,
                                 out_path_hourly=RESULT_XHWI,
                                 out_path_diary=RESULT_XHWI_DIARY_INDPROD,
                                 out_path_monthly=RESULT_XHWI_MONTH_INDPROD,
                                 )

        logger.info("✅ Pipeline completed successfully.")

    except Exception as e:
        logger.exception(f"❌ Pipeline failed: {e}")

    finally:
        logger.info("🧹 Closing Dask client and releasing resources.")
        stop()
