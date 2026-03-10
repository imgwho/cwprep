"""
cwprep config unit tests.
"""

import pytest

pytest.importorskip("yaml")

from cwprep.config import load_config


def test_load_config_uses_2024_2_defaults_when_prep_missing(workspace_tmp_dir, monkeypatch):
    monkeypatch.delenv("DB_USERNAME", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)

    yaml_path = workspace_tmp_dir / "config.yaml"
    yaml_path.write_text(
        "tableau_server:\n  url: http://localhost\n",
        encoding="utf-8",
    )

    config = load_config(yaml_path=str(yaml_path), auto_load_env=False)

    assert config.prep_version == "2024.2.0"
    assert config.prep_year == 2024
    assert config.prep_quarter == 2
    assert config.prep_release == 0


def test_load_config_reads_explicit_prep_override(workspace_tmp_dir, monkeypatch):
    monkeypatch.setenv("DB_USERNAME", "analyst")
    monkeypatch.setenv("DB_PASSWORD", "secret")

    yaml_path = workspace_tmp_dir / "config.yaml"
    yaml_path.write_text(
        "database:\n"
        "  host: db.local\n"
        "  dbname: analytics\n"
        "  type: postgres\n"
        "prep:\n"
        "  version: 2024.3.1\n"
        "  year: 2024\n"
        "  quarter: 3\n"
        "  release: 1\n",
        encoding="utf-8",
    )

    config = load_config(yaml_path=str(yaml_path), auto_load_env=False)

    assert config.database is not None
    assert config.database.host == "db.local"
    assert config.database.dbname == "analytics"
    assert config.database.db_class == "postgres"
    assert config.database.username == "analyst"
    assert config.database.password == "secret"
    assert config.prep_version == "2024.3.1"
    assert config.prep_year == 2024
    assert config.prep_quarter == 3
    assert config.prep_release == 1
