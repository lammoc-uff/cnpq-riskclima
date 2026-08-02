from src.config.settings import CALIB_OUT

from concat_months import main as concat_months
from make_calibration import main as make_calibration
from run_months import main as run_months


def main() -> None:
    if CALIB_OUT.exists():
        print(f"Calibration file already exists, skipping: {CALIB_OUT}")
    else:
        make_calibration()

    run_months()
    concat_months()


if __name__ == "__main__":
    main()
