from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
_TEST_TMP_ROOT = _REPO_ROOT / ".tmp_pytest_workspace"


@pytest.fixture(scope="session")
def workspace_tmp_root():
    """Create a writable temp root inside the repository workspace."""
    _TEST_TMP_ROOT.mkdir(exist_ok=True)
    yield _TEST_TMP_ROOT
    shutil.rmtree(_TEST_TMP_ROOT, ignore_errors=True)


@pytest.fixture
def workspace_tmp_dir(workspace_tmp_root):
    """Create an isolated temp directory under the workspace temp root."""
    path = workspace_tmp_root / f"cwprep_{uuid.uuid4().hex}"
    path.mkdir()
    yield path
    shutil.rmtree(path, ignore_errors=True)
