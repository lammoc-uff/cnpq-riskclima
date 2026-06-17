from src.preprocessing.zarr_writer import process_variable
from src.config.settings import RAW, ZARR_T2M, PERIOD_NAME

if __name__ == "__main__":
    process_variable(
        input_dir=str(RAW / f"t2m_prev_{PERIOD_NAME}"),
        output_zarr=str(ZARR_T2M),
        var_name="t2m",
        kelvin_to_celsius=True,
        chunks="auto",
    )
