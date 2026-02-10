from core.builder import TFLBuilder
from core.packager import TFLPackager

# 1. 初始化
builder = TFLBuilder(flow_name="Ultimate_V2_Test")
conn_id = builder.add_connection("rm-uf661w842h18w326nvo.mysql.rds.aliyuncs.com", "link_BI", "voxadmin")

# 2. 添加两个 SQL 输入 (fields 设为 None，pk 不注册)
sql_order = "SELECT o.user_id AS 客户ID, o.id AS 订单ID, o.acc AS 员工ID FROM e_product_order_list o WHERE trash = 0 LIMIT 100"
order_inp = builder.add_input_sql("e_product_order_list", sql_order, conn_id)

sql_admin = "SELECT id, username FROM bi_e_admin LIMIT 100"
admin_inp = builder.add_input_sql("bi_e_admin", sql_admin, conn_id)

# 3. 联接 (此处自动注册 PK)
join_node = builder.add_join("联接 1", order_inp, admin_inp, "员工ID", "id")

# 4. 输出
builder.add_output_server("输出", join_node, "SDK_Ultimate_V2_Result")

# 5. 生成并打包
flow, display, meta = builder.build()
output_dir = "workspace/output/ultimate_v2"
tfl_file = "workspace/output/ultimate_v2.tfl"

TFLPackager.save_to_folder(output_dir, flow, display, meta)
TFLPackager.pack_zip(output_dir, tfl_file)

print(f"究极 V2 测试完成！请验证：{tfl_file}")
