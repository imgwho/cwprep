"""
cwprep 数据清理功能演示

业务场景：盈利订单筛选与分析
- 筛选利润 > 0 的订单
- 计算利润率
- 重命名为中文字段

功能覆盖：
1. add_filter - 表达式筛选
2. add_value_filter - 值筛选
3. add_keep_only - 只保留列
4. add_remove_columns - 移除列
5. add_rename - 重命名
6. add_calculation - 计算字段

使用方法：
    python examples/demo_cleaning.py
"""

from cwprep import TFLBuilder, TFLPackager

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_cleaning_demo():
    print("=" * 50)
    print("cwprep 数据清理功能演示")
    print("=" * 50)
    print()
    
    builder = TFLBuilder(flow_name="Profitable Orders Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"✅ 添加数据库连接")
    
    # 添加订单表（包含产品信息）
    orders_id = builder.add_input_sql(
        name="Orders with Products",
        sql="""
        SELECT 
            o.order_id,
            o.order_date,
            o.ship_mode,
            o.customer_id,
            o.city,
            o.state,
            p.product_name,
            p.category,
            p.sub_category,
            o.sales,
            o.quantity,
            o.discount,
            o.profit
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        """,
        connection_id=conn_id
    )
    print(f"✅ [1] add_input_sql: 添加订单+产品关联表")
    
    # 1. 表达式筛选：利润大于0
    filter1_id = builder.add_filter(
        name="Filter Profitable",
        parent_id=orders_id,
        expression="[profit] > 0"
    )
    print(f"✅ [2] add_filter: 筛选 profit > 0")
    
    # 2. 值筛选：只保留特定配送方式
    filter2_id = builder.add_value_filter(
        name="Filter Ship Mode",
        parent_id=filter1_id,
        field="ship_mode",
        values=["Standard Class", "First Class"],
        exclude=False
    )
    print(f"✅ [3] add_value_filter: 保留 Standard/First Class")
    
    # 3. 只保留核心列
    keep_id = builder.add_keep_only(
        name="Keep Core Fields",
        parent_id=filter2_id,
        columns=["order_id", "order_date", "customer_id", "category", 
                 "product_name", "sales", "quantity", "profit"]
    )
    print(f"✅ [4] add_keep_only: 保留 8 个核心字段")
    
    # 4. 重命名为中文
    rename_id = builder.add_rename(
        parent_id=keep_id,
        renames={
            "order_id": "订单编号",
            "order_date": "订单日期",
            "customer_id": "客户编号",
            "category": "产品大类",
            "product_name": "产品名称",
            "sales": "销售额",
            "quantity": "数量",
            "profit": "利润"
        }
    )
    print(f"✅ [5] add_rename: 重命名为中文")
    
    # 5. 计算利润率
    calc1_id = builder.add_calculation(
        name="Calculate Profit Rate",
        parent_id=rename_id,
        column_name="利润率",
        formula="[利润] / [销售额]"
    )
    print(f"✅ [6] add_calculation: 计算利润率 = 利润/销售额")
    
    # 6. 计算订单等级
    calc2_id = builder.add_calculation(
        name="Calculate Order Level",
        parent_id=calc1_id,
        column_name="订单等级",
        formula="IF [销售额] >= 500 THEN 'High' ELSEIF [销售额] >= 100 THEN 'Medium' ELSE 'Low' END"
    )
    print(f"✅ [7] add_calculation: 计算订单等级")
    
    # 7. 移除客户编号（脱敏）
    remove_id = builder.add_remove_columns(
        name="Remove Customer ID",
        parent_id=calc2_id,
        columns=["客户编号"]
    )
    print(f"✅ [8] add_remove_columns: 移除客户编号")
    
    # 输出
    output_id = builder.add_output_server(
        name="Output",
        parent_id=remove_id,
        datasource_name="Profitable_Orders"
    )
    print(f"✅ [9] add_output_server: 添加输出")
    
    # 构建
    print()
    flow, display, meta = builder.build()
    
    output_folder = "./demo_output/cleaning"
    output_tfl = "./demo_output/cleaning.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"✅ 已生成: {output_tfl}")
    print()
    print("功能覆盖:")
    print("  • add_filter (profit > 0)")
    print("  • add_value_filter (ship_mode)")
    print("  • add_keep_only")
    print("  • add_rename")
    print("  • add_calculation (利润率, 订单等级)")
    print("  • add_remove_columns")


if __name__ == "__main__":
    run_cleaning_demo()
