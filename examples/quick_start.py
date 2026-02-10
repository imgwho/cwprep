"""
cwprep 快速开始示例

这个示例展示如何使用 cwprep SDK 创建一个简单的 Tableau Prep 数据流程。

使用方法:
    1. 安装 cwprep: pip install cwprep
    2. 运行此脚本: python quick_start.py
    3. 在 Tableau Prep 中打开生成的 .tfl 文件
"""

from cwprep import TFLBuilder, TFLPackager

def main():
    print("=" * 50)
    print("cwprep 快速开始示例")
    print("=" * 50)
    print()
    
    # 1. 创建 Builder
    builder = TFLBuilder(flow_name="Demo Flow")
    print("✅ 创建 Builder")
    
    # 2. 添加数据库连接
    # 注意：这里使用占位符，实际使用时请替换为真实的数据库信息
    conn_id = builder.add_connection(
        host="localhost",
        username="root",
        dbname="demo_db"
    )
    print(f"✅ 添加数据库连接: {conn_id[:8]}...")
    
    # 3. 添加输入表
    users_id = builder.add_input_sql(
        name="Users",
        sql="SELECT id, name, email, created_at FROM users",
        connection_id=conn_id
    )
    print(f"✅ 添加输入表 Users: {users_id[:8]}...")
    
    orders_id = builder.add_input_sql(
        name="Orders", 
        sql="SELECT id, user_id, amount, status FROM orders",
        connection_id=conn_id
    )
    print(f"✅ 添加输入表 Orders: {orders_id[:8]}...")
    
    # 4. 联接两张表
    join_id = builder.add_join(
        name="Users + Orders",
        left_id=users_id,
        right_id=orders_id,
        left_col="id",
        right_col="user_id",
        join_type="left"
    )
    print(f"✅ 添加联接节点: {join_id[:8]}...")
    
    # 5. 添加筛选条件
    filter_id = builder.add_filter(
        name="过滤有效订单",
        parent_id=join_id,
        expression="[status] = 'completed'"
    )
    print(f"✅ 添加筛选器: {filter_id[:8]}...")
    
    # 6. 添加计算字段
    calc_id = builder.add_calculation(
        name="订单等级",
        parent_id=filter_id,
        column_name="order_level",
        formula="IF [amount] >= 1000 THEN 'High' ELSEIF [amount] >= 100 THEN 'Medium' ELSE 'Low' END"
    )
    print(f"✅ 添加计算字段: {calc_id[:8]}...")
    
    # 7. 添加输出
    output_id = builder.add_output_server(
        name="Output",
        parent_id=calc_id,
        datasource_name="Demo_Output"
    )
    print(f"✅ 添加输出节点: {output_id[:8]}...")
    
    # 8. 构建并保存
    print()
    print("-" * 50)
    flow, display, meta = builder.build()
    print("✅ 构建完成")
    
    # 保存到文件夹
    output_folder = "./demo_output"
    output_tfl = "./demo_output.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print()
    print("=" * 50)
    print(f"✅ 成功生成: {output_tfl}")
    print()
    print("下一步:")
    print("  1. 用 Tableau Prep 打开 demo_output.tfl")
    print("  2. 配置数据库连接")
    print("  3. 运行流程验证")
    print("=" * 50)


if __name__ == "__main__":
    main()
