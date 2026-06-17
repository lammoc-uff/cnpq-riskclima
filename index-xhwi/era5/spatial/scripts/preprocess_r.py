from src.preprocessing.zarr_writer import process_variable

if __name__ == "__main__":
    process_variable(
        input_dir="/home/mpas/Documents/data/era5/heatwaves/r_prev_1950-2024",
        output_zarr="/home/mpas/Documents/data/era5/heatwaves/r_prev_1950-2024/zarr_store_humidity_br_1950-2024",
        var_name="r",
        kelvin_to_celsius=False,
        chunks="auto",
    )