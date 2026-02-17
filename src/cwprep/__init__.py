"""
cwprep - Tableau Prep Flow SDK

Programmatically generate Tableau Prep data flow files (.tfl).

Usage:
    from cwprep import TFLBuilder, TFLPackager
    
    builder = TFLBuilder(flow_name="My Flow")
    conn_id = builder.add_connection(host="localhost", username="root", dbname="mydb")
    input1 = builder.add_input_sql("Users", "SELECT * FROM users", conn_id)
    builder.add_output_server("Output", input1, "Datasource Name")
    
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
    DEFAULT_CONFIG
)

__version__ = "0.2.0"
__author__ = "cooper wenhua"
__all__ = [
    "TFLBuilder", 
    "TFLPackager", 
    "TFLConfig", 
    "DatabaseConfig", 
    "TableauServerConfig", 
    "load_config",
    "DEFAULT_CONFIG",
    "__version__"
]

# MCP Server is available via `cwprep.mcp_server` (requires `pip install cwprep[mcp]`)
# Usage: cwprep-mcp  or  python -m cwprep.mcp_server
