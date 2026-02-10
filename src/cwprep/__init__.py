"""
cwprep - Tableau Prep 数据流程 SDK

用于程序化生成 Tableau Prep 数据流程文件 (.tfl)。

使用示例:
    from cwprep import TFLBuilder, TFLPackager
    
    builder = TFLBuilder(flow_name="我的流程")
    conn_id = builder.add_connection(host="localhost", username="root", dbname="mydb")
    input1 = builder.add_input_sql("Users", "SELECT * FROM users", conn_id)
    builder.add_output_server("Output", input1, "数据源名")
    
    flow, display, meta = builder.build()
    TFLPackager.save_to_folder("./output", flow, display, meta)
    TFLPackager.pack_zip("./output", "./output.tfl")
"""

from .builder import TFLBuilder
from .packager import TFLPackager
from .config import (
    TFLConfig, 
    DatabaseConfig, 
    TableauServerConfig, 
    load_config,
    DEFAULT_CONFIG,
    LOCAL_CONFIG
)

__version__ = "0.1.0"
__author__ = "imgwho"
__all__ = [
    "TFLBuilder", 
    "TFLPackager", 
    "TFLConfig", 
    "DatabaseConfig", 
    "TableauServerConfig", 
    "load_config",
    "DEFAULT_CONFIG",
    "LOCAL_CONFIG",
    "__version__"
]
