from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
DOCS_DIR = PROJECT_ROOT / "docs"
LOGS_DIR = PROJECT_ROOT / "logs"


def project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


if __name__ == "__main__":
    print(PROJECT_ROOT)
    print(DATA_DIR)
    print(DOCS_DIR)
    print(LOGS_DIR)
