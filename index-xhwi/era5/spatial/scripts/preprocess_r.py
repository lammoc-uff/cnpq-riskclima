from src.preprocessing.zarr_writer import process_variable
from src.config.settings import RAW, ZARR_R, PERIOD_NAME

if __name__ == "__main__":
    process_variable(
        input_dir=str(RAW / f"r_prev_{PERIOD_NAME}"),
        output_zarr=str(ZARR_R),
        var_name="r",
        kelvin_to_celsius=False,
        chunks="auto",
    )
