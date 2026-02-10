"""
TFL Generator SDK 测试脚本

演示两种使用方式：
1. 使用配置文件中的默认连接
2. 手动指定连接参数
"""

from core.builder import TFLBuilder
from core.packager import TFLPackager
from core.config import DEFAULT_CONFIG

# ============ 方式1: 使用默认配置 ============
print("=== 使用默认配置创建流程 ===")

# 初始化（自动使用 DEFAULT_CONFIG）
builder = TFLBuilder(flow_name="Ultimate_V2_Test")

# 使用配置中的默认数据库连接
conn_id = builder.add_connection_from_config()

# 添加 SQL 输入
sql_order = "SELECT o.user_id AS 客户ID, o.id AS 订单ID, o.acc AS 员工ID FROM e_product_order_list o WHERE trash = 0 LIMIT 100"
order_inp = builder.add_input_sql("e_product_order_list", sql_order, conn_id)

sql_admin = "SELECT id, username FROM bi_e_admin LIMIT 100"
admin_inp = builder.add_input_sql("bi_e_admin", sql_admin, conn_id)

# 联接
join_node = builder.add_join("联接 1", order_inp, admin_inp, "员工ID", "id")

# 输出（自动使用配置中的 Server URL）
builder.add_output_server("输出", join_node, "SDK_Ultimate_V2_Result")

# 生成并打包
flow, display, meta = builder.build()
output_dir = "workspace/output/ultimate_v2"
tfl_file = "workspace/output/ultimate_v2.tfl"

TFLPackager.save_to_folder(output_dir, flow, display, meta)
TFLPackager.pack_zip(output_dir, tfl_file)

print(f"✅ 测试完成！生成文件：{tfl_file}")
print(f"   Server URL: {builder.config.server.server_url}")
print(f"   数据库: {builder.config.database.host}")
