"""
cwprep 配置模块

支持三种配置来源（优先级从高到低）：
1. 代码中直接传入的参数
2. 环境变量 (.env 文件)
3. YAML 配置文件 (config.yaml)

使用示例：
    from cwprep import load_config, TFLBuilder
    
    # 自动加载 config.yaml 和 .env
    config = load_config()
    
    # 使用配置
    builder = TFLBuilder(flow_name="流程", config=config)
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

# 尝试导入可选依赖
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


# ============ 数据类定义 ============

@dataclass
class DatabaseConfig:
    """数据库连接配置"""
    host: str = ""
    port: str = "3306"
    username: str = ""
    password: str = ""
    dbname: str = ""
    db_class: str = "mysql"


@dataclass
class TableauServerConfig:
    """Tableau Server 配置"""
    server_url: str = "http://localhost"
    default_project: str = "Default"
    project_luid: str = ""


@dataclass
class TFLConfig:
    """TFL 生成器主配置"""
    server: TableauServerConfig = field(default_factory=TableauServerConfig)
    database: Optional[DatabaseConfig] = None
    prep_version: str = "2019.1.3"
    prep_year: int = 2019
    prep_quarter: int = 1
    prep_release: int = 3


# ============ 配置加载函数 ============

def _find_config_path() -> Path:
    """查找配置文件路径
    
    搜索顺序：
    1. 当前工作目录
    2. 从当前目录向上查找包含 config.yaml 的目录
    """
    # 从当前工作目录开始搜索
    current = Path.cwd()
    
    # 先检查当前目录
    if (current / "config.yaml").exists():
        return current
    
    # 向上遍历查找
    for parent in current.parents:
        if (parent / "config.yaml").exists():
            return parent
    
    # 找不到时返回当前目录
    return current


def load_config(
    yaml_path: str = None,
    env_path: str = None,
    auto_load_env: bool = True
) -> TFLConfig:
    """
    加载配置
    
    Args:
        yaml_path: YAML 配置文件路径（默认自动查找 config.yaml）
        env_path: .env 文件路径（默认自动查找 .env）
        auto_load_env: 是否自动加载 .env 文件
        
    Returns:
        TFLConfig: 配置对象
    """
    config_dir = _find_config_path()
    
    # 1. 加载 .env 文件
    if auto_load_env and HAS_DOTENV:
        env_file = Path(env_path) if env_path else config_dir / ".env"
        if env_file.exists():
            load_dotenv(env_file)
    
    # 2. 加载 YAML 配置
    yaml_data = {}
    if HAS_YAML:
        yaml_file = Path(yaml_path) if yaml_path else config_dir / "config.yaml"
        if yaml_file.exists():
            with open(yaml_file, "r", encoding="utf-8") as f:
                yaml_data = yaml.safe_load(f) or {}
    
    # 3. 构建配置对象
    # Tableau Server 配置
    server_yaml = yaml_data.get("tableau_server", {})
    server_config = TableauServerConfig(
        server_url=server_yaml.get("url", "http://localhost"),
        default_project=server_yaml.get("default_project", "Default"),
        project_luid=server_yaml.get("project_luid", "")
    )
    
    # 数据库配置（YAML + 环境变量）
    db_yaml = yaml_data.get("database", {})
    db_config = DatabaseConfig(
        host=db_yaml.get("host", ""),
        port=str(db_yaml.get("port", 3306)),
        username=os.getenv("DB_USERNAME", ""),
        password=os.getenv("DB_PASSWORD", ""),
        dbname=db_yaml.get("dbname", ""),
        db_class=db_yaml.get("type", "mysql")
    )
    
    # Prep 版本配置
    prep_yaml = yaml_data.get("prep", {})
    
    return TFLConfig(
        server=server_config,
        database=db_config if db_config.host else None,
        prep_version=prep_yaml.get("version", "2019.1.3"),
        prep_year=prep_yaml.get("year", 2019),
        prep_quarter=prep_yaml.get("quarter", 1),
        prep_release=prep_yaml.get("release", 3)
    )


# 预加载默认配置
try:
    DEFAULT_CONFIG = load_config()
except Exception:
    # 如果加载失败，使用空配置（用户需要显式配置）
    DEFAULT_CONFIG = TFLConfig(
        server=TableauServerConfig(),
        database=None
    )
