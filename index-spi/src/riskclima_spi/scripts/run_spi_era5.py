import logging

from riskclima_spi.config import ERA5Settings
from riskclima_spi.era5 import run_era5


def main() -> None:
    """Acquire ERA5 monthly precipitation when needed and calculate SPI."""
    settings = ERA5Settings()
    logging.basicConfig(level=settings.log_level, format=settings.log_format)
    output_path = run_era5(settings)
    logging.getLogger(__name__).info("SPI output written to %s", output_path)


if __name__ == "__main__":
    main()
