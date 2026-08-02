from src.config.settings import MONTHLY_OUTPUT_FILE, MONTHLY_PARTS_DIR
from src.io.writers import concat_monthly_netcdfs


def main() -> None:
    part_files = sorted(MONTHLY_PARTS_DIR.glob("xhwi_era5_month*.nc"))
    final_output = concat_monthly_netcdfs(
        input_paths=part_files,
        output_path=MONTHLY_OUTPUT_FILE,
        overwrite=True,
    )
    print("Final output written:")
    print(final_output)


if __name__ == "__main__":
    main()
