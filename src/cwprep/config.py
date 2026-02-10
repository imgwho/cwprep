"""
cwprep Configuration Module

Supports three configuration sources (priority from high to low):
1. Parameters passed directly in code
2. Environment variables (.env file)
3. YAML configuration file (config.yaml)

Usage:
    from cwprep import load_config, TFLBuilder
    
    # Auto-load config.yaml and .env
    config = load_config()
    
    # Use configuration
    builder = TFLBuilder(flow_name="MyFlow", config=config)
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

# Try to import optional dependencies
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
    yaml = None

try:
    from dotenv import load_dotenv
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False


# ============ Dataclass Definitions ============

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = ""
    port: str = "3306"
    username: str = ""
    password: str = ""
    dbname: str = ""
    db_class: str = "mysql"


@dataclass
class TableauServerConfig:
    """Tableau Server configuration"""
    server_url: str = "http://localhost"
    default_project: str = "Default"
    project_luid: str = ""


@dataclass
class TFLConfig:
    """TFL Generator main configuration"""
    server: TableauServerConfig = field(default_factory=TableauServerConfig)
    database: Optional[DatabaseConfig] = None
    prep_version: str = "2019.1.3"
    prep_year: int = 2019
    prep_quarter: int = 1
    prep_release: int = 3


# ============ Configuration Loading Functions ============

def _find_config_path() -> Path:
    """Find configuration file path
    
    Search order:
    1. Current working directory
    2. Search upward from current directory for config.yaml
    """
    # Start searching from current working directory
    current = Path.cwd()
    
    # First check current directory
    if (current / "config.yaml").exists():
        return current
    
    # Traverse upward
    for parent in current.parents:
        if (parent / "config.yaml").exists():
            return parent
    
    # Return current directory if not found
    return current


def load_config(
    yaml_path: str = None,
    env_path: str = None,
    auto_load_env: bool = True
) -> TFLConfig:
    """
    Load configuration
    
    Args:
        yaml_path: YAML config file path (defaults to auto-find config.yaml)
        env_path: .env file path (defaults to auto-find .env)
        auto_load_env: Whether to auto-load .env file
        
    Returns:
        TFLConfig: Configuration object
    """
    config_dir = _find_config_path()
    
    # 1. Load .env file
    if auto_load_env and HAS_DOTENV:
        env_file = Path(env_path) if env_path else config_dir / ".env"
        if env_file.exists():
            load_dotenv(env_file)
    
    # 2. Load YAML configuration
    yaml_data = {}
    if HAS_YAML:
        yaml_file = Path(yaml_path) if yaml_path else config_dir / "config.yaml"
        if yaml_file.exists():
            with open(yaml_file, "r", encoding="utf-8") as f:
                yaml_data = yaml.safe_load(f) or {}
    
    # 3. Build configuration object
    # Tableau Server configuration
    server_yaml = yaml_data.get("tableau_server", {})
    server_config = TableauServerConfig(
        server_url=server_yaml.get("url", "http://localhost"),
        default_project=server_yaml.get("default_project", "Default"),
        project_luid=server_yaml.get("project_luid", "")
    )
    
    # Database configuration (YAML + environment variables)
    db_yaml = yaml_data.get("database", {})
    db_config = DatabaseConfig(
        host=db_yaml.get("host", ""),
        port=str(db_yaml.get("port", 3306)),
        username=os.getenv("DB_USERNAME", ""),
        password=os.getenv("DB_PASSWORD", ""),
        dbname=db_yaml.get("dbname", ""),
        db_class=db_yaml.get("type", "mysql")
    )
    
    # Prep version configuration
    prep_yaml = yaml_data.get("prep", {})
    
    return TFLConfig(
        server=server_config,
        database=db_config if db_config.host else None,
        prep_version=prep_yaml.get("version", "2019.1.3"),
        prep_year=prep_yaml.get("year", 2019),
        prep_quarter=prep_yaml.get("quarter", 1),
        prep_release=prep_yaml.get("release", 3)
    )


# Pre-load default configuration
try:
    DEFAULT_CONFIG = load_config()
except Exception:
    # If loading fails, use empty config (user needs to configure explicitly)
    DEFAULT_CONFIG = TFLConfig(
        server=TableauServerConfig(),
        database=None
    )
