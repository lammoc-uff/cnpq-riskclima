from src.config.settings import CALIB_OUT, ERA5_ZARR_PATH, MONTHS_TO_RUN
from src.io.writers import monthly_part_path, write_monthly_netcdf
from src.pipeline.monthly_pipeline import compute_era5_monthly_xhwi_torch


def main() -> None:
    part_output = monthly_part_path(MONTHS_TO_RUN)

    if part_output.exists():
        print(f"File already exists, skipping processing: {part_output}")
        return

    ds_era5_monthly = compute_era5_monthly_xhwi_torch(
        ERA5_ZARR_PATH,
        calibration_path=CALIB_OUT,
        months=MONTHS_TO_RUN,
    )

    written_path = write_monthly_netcdf(ds_era5_monthly, output_path=part_output)
    print("Written:")
    print(written_path)


if __name__ == "__main__":
    main()
