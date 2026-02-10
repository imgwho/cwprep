"""
cwprep 基础功能演示：输入、联接、输出

业务场景：客户订单分析
- 订单表关联客户表
- 展示客户购买记录

使用方法：
    python examples/demo_basic.py
"""

from cwprep import TFLBuilder, TFLPackager

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_basic_demo():
    print("=" * 50)
    print("cwprep 基础功能演示：客户订单关联")
    print("=" * 50)
    print()
    
    # 1. 创建 Builder 和连接
    builder = TFLBuilder(flow_name="Customer Orders Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"✅ 添加数据库连接: {conn_id[:8]}...")
    
    # 2. 添加订单表
    orders_id = builder.add_input_sql(
        name="Orders",
        sql="""
        SELECT 
            order_id,
            order_date,
            ship_mode,
            customer_id,
            city,
            state,
            sales,
            quantity,
            profit
        FROM orders
        """,
        connection_id=conn_id
    )
    print(f"✅ 添加订单表: {orders_id[:8]}...")
    
    # 3. 添加客户表
    customers_id = builder.add_input_sql(
        name="Customers",
        sql="""
        SELECT 
            customer_id,
            customer_name,
            segment
        FROM customers
        """,
        connection_id=conn_id
    )
    print(f"✅ 添加客户表: {customers_id[:8]}...")
    
    # 4. 联接订单和客户
    join_id = builder.add_join(
        name="Orders + Customers",
        left_id=orders_id,
        right_id=customers_id,
        left_col="customer_id",
        right_col="customer_id",
        join_type="left"
    )
    print(f"✅ 添加联接: {join_id[:8]}...")
    
    # 5. 添加输出
    output_id = builder.add_output_server(
        name="Output",
        parent_id=join_id,
        datasource_name="Customer_Orders"
    )
    print(f"✅ 添加输出: {output_id[:8]}...")
    
    # 6. 构建和保存
    print()
    flow, display, meta = builder.build()
    
    output_folder = "./demo_output/basic"
    output_tfl = "./demo_output/basic.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"✅ 已生成: {output_tfl}")
    print()
    print("数据流: Orders + Customers → Join → Output")


if __name__ == "__main__":
    run_basic_demo()
