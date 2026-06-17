from src.preprocessing.zarr_writer import process_variable

if __name__ == "__main__":
    process_variable(
        input_dir="/home/mpas/Documents/data/era5/heatwaves/t2m_prev_1950-2024",
        output_zarr="/home/mpas/Documents/data/era5/heatwaves/t2m_prev_1950-2024/zarr_store_t2m_br_1950-2024",
        var_name="t2m",
        kelvin_to_celsius=True,
        chunks="auto",
    )