"""Static checks for exploratory notebooks."""

import json
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATHS = (
    PROJECT_ROOT / "notebooks/explorer.ipynb",
    PROJECT_ROOT / "notebooks/explorer_add_preprocessing.ipynb",
    PROJECT_ROOT / "notebooks/explorer_oo.ipynb",
)
PERSONAL_PATH = re.compile(
    r"(?:[A-Za-z]:\\Users\\|/home/[^/]+/|/Users/[^/]+/|\b(?:usuario|lammoc)\b)",
    re.IGNORECASE,
)
PORTUGUESE_MARKERS = re.compile(
    r"\b(?:arquivo|catálogo|caminho|coordenadas|diferença|dimensão|erro|executando|"
    r"experimento|falha|frequência|membro|mensagem|modelo|núcleos|ordenar|possui|"
    r"processando|pulando|recorte|responsável|resultado|salvar|sucesso|variável|versão)\b",
    re.IGNORECASE,
)


@pytest.mark.parametrize("path", NOTEBOOK_PATHS, ids=lambda path: path.name)
def test_notebook_is_clean_and_compilable(path: Path) -> None:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    assert notebook["nbformat"] == 4

    all_source: list[str] = []
    for cell_number, cell in enumerate(notebook["cells"]):
        source = "".join(cell.get("source", []))
        all_source.append(source)
        if cell["cell_type"] != "code":
            continue
        assert cell.get("execution_count") is None
        assert cell.get("outputs") == []
        compilable = "".join(
            line
            for line in source.splitlines(keepends=True)
            if not line.lstrip().startswith(("%", "!", "?"))
        )
        compile(compilable, f"{path.name}:cell-{cell_number}", "exec")

    content = "\n".join(all_source)
    assert PERSONAL_PATH.search(content) is None
    assert PORTUGUESE_MARKERS.search(content) is None
