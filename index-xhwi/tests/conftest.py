from collections.abc import Iterator
from pathlib import Path

import pytest
from dotenv import dotenv_values

ENV_EXAMPLE = Path(__file__).parents[1] / ".env.example"


@pytest.fixture(autouse=True)
def configured_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Provide the complete canonical example environment to every test."""
    for name, value in dotenv_values(ENV_EXAMPLE).items():
        if value is not None:
            monkeypatch.setenv(name, value)
    yield
