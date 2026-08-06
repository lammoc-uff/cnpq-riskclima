import logging

from riskclima_spi.cmip6 import run_cmip6
from riskclima_spi.config import CMIP6Settings


def main() -> None:
    """Run SPI for the CMIP6 input described by ``.env``."""
    settings = CMIP6Settings()
    logging.basicConfig(level=settings.log_level, format=settings.log_format)
    output_path = run_cmip6(settings)
    logging.getLogger(__name__).info("SPI output written to %s", output_path)


if __name__ == "__main__":
    main()
