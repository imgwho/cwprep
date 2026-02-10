"""
cwprep 综合演示：销售分析数据流

覆盖 cwprep SDK 的全部核心功能（含 add_input_table）：

1. add_connection - 数据库连接
2. add_input_table - 原表输入（客户、区域、产品、退货）
3. add_input_sql - SQL 输入（1月/2月订单）
4. add_union - 并集合并
5. add_join - 多表联接 (x3)
6. add_filter - 表达式筛选
7. add_value_filter - 值筛选
8. add_keep_only - 只保留列
9. add_remove_columns - 移除列
10. add_rename - 重命名
11. add_calculation - 计算字段 (x2)
12. add_aggregate - 聚合统计
13. add_pivot - 行转列
14. add_unpivot - 列转行
15. add_clean_step - 清理步骤
16. add_output_server - 服务器输出

业务场景：
- 合并1月和2月订单（排除退货）
- 关联客户、产品、区域主表
- 计算利润率、客户等级
- 按区域月度汇总分析
- 生成月度对比报表

使用方法：
    python examples/demo_comprehensive.py
"""

from cwprep import TFLBuilder, TFLPackager

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_comprehensive_demo():
    print("=" * 60)
    print("cwprep 综合演示：销售分析数据流")
    print("=" * 60)
    print()
    
    # ========================================
    # 1. 初始化 Builder 和连接
    # ========================================
    builder = TFLBuilder(flow_name="Superstore Sales Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"✅ [1] add_connection: {conn_id[:8]}...")
    
    # ========================================
    # 2. 输入源：1月订单（自定义 SQL，需要 WHERE 条件）
    # ========================================
    jan_orders_id = builder.add_input_sql(
        name="Orders_Jan_2024",
        sql="""
        SELECT 
            '2024-01' AS month_label,
            order_id,
            order_date,
            ship_mode,
            customer_id,
            region_id,
            city,
            state,
            product_id,
            sales,
            quantity,
            discount,
            profit
        FROM orders 
        WHERE order_date >= '2024-01-01'
          AND order_date < '2024-02-01'
        """,
        connection_id=conn_id
    )
    print(f"✅ [2] add_input_sql (1月订单): {jan_orders_id[:8]}...")
    
    # ========================================
    # 3. 输入源：2月订单（自定义 SQL）
    # ========================================
    feb_orders_id = builder.add_input_sql(
        name="Orders_Feb_2024",
        sql="""
        SELECT 
            '2024-02' AS month_label,
            order_id,
            order_date,
            ship_mode,
            customer_id,
            region_id,
            city,
            state,
            product_id,
            sales,
            quantity,
            discount,
            profit
        FROM orders 
        WHERE order_date >= '2024-02-01'
          AND order_date < '2024-03-01'
        """,
        connection_id=conn_id
    )
    print(f"✅ [3] add_input_sql (2月订单): {feb_orders_id[:8]}...")
    
    # ========================================
    # 4. 输入源：客户表（原表连接）
    # ========================================
    customers_id = builder.add_input_table(
        name="customers",
        table_name="customers",
        connection_id=conn_id
    )
    print(f"✅ [4] add_input_table (客户表): {customers_id[:8]}...")
    
    # ========================================
    # 5. 输入源：区域表（原表连接）
    # ========================================
    regions_id = builder.add_input_table(
        name="regions",
        table_name="regions",
        connection_id=conn_id
    )
    print(f"✅ [5] add_input_table (区域表): {regions_id[:8]}...")
    
    # ========================================
    # 6. 输入源：退货表（原表连接）
    # ========================================
    returns_id = builder.add_input_table(
        name="returns",
        table_name="returns",
        connection_id=conn_id
    )
    print(f"✅ [6] add_input_table (退货表): {returns_id[:8]}...")
    
    # ========================================
    # 7. 并集：合并1月和2月订单
    # ========================================
    union_id = builder.add_union(
        name="Union_Jan_Feb",
        parent_ids=[jan_orders_id, feb_orders_id]
    )
    print(f"✅ [7] add_union (合并订单): {union_id[:8]}...")
    
    # ========================================
    # 8. 联接1：订单关联客户
    # ========================================
    join1_id = builder.add_join(
        name="Orders_Customers",
        left_id=union_id,
        right_id=customers_id,
        left_col="customer_id",
        right_col="customer_id",
        join_type="left"
    )
    print(f"✅ [8] add_join (订单+客户): {join1_id[:8]}...")
    
    # ========================================
    # 9. 联接2：关联区域表
    # ========================================
    join2_id = builder.add_join(
        name="Orders_Customers_Regions",
        left_id=join1_id,
        right_id=regions_id,
        left_col="region_id",
        right_col="region_id",
        join_type="left"
    )
    print(f"✅ [9] add_join (关联区域): {join2_id[:8]}...")
    
    # ========================================
    # 10. 只保留核心列（在退货联接前精简）
    # ========================================
    keep_only_id = builder.add_keep_only(
        name="Keep_Core_Fields",
        parent_id=join2_id,
        columns=["month_label", "order_id", "order_date", "ship_mode",
                 "customer_name", "segment", "region_name", "manager_name",
                 "city", "state", "sales", "quantity", "discount", "profit"]
    )
    print(f"✅ [10] add_keep_only (保留核心字段): {keep_only_id[:8]}...")
    
    # ========================================
    # 11. 重命名列
    # ========================================
    rename_id = builder.add_rename(
        parent_id=keep_only_id,
        renames={
            "month_label": "Month",
            "customer_name": "Customer",
            "segment": "Segment",
            "region_name": "Region",
            "manager_name": "Manager",
            "ship_mode": "Ship_Mode"
        }
    )
    print(f"✅ [11] add_rename: {rename_id[:8]}...")
    
    # ========================================
    # 12. 值筛选：排除 Same Day 配送
    # ========================================
    value_filter_id = builder.add_value_filter(
        name="Filter_Ship_Mode",
        parent_id=rename_id,
        field="Ship_Mode",
        values=["Same Day"],
        exclude=True
    )
    print(f"✅ [12] add_value_filter (排除Same Day): {value_filter_id[:8]}...")
    
    # ========================================
    # 13. 计算利润率
    # ========================================
    calc1_id = builder.add_calculation(
        name="Calc_Profit_Rate",
        parent_id=value_filter_id,
        column_name="Profit_Rate",
        formula="IF [sales] = 0 THEN 0 ELSE [profit] / [sales] END"
    )
    print(f"✅ [13] add_calculation (利润率): {calc1_id[:8]}...")
    
    # ========================================
    # 14. 计算订单等级
    # ========================================
    calc2_id = builder.add_calculation(
        name="Calc_Order_Level",
        parent_id=calc1_id,
        column_name="Order_Level",
        formula="IF [sales] >= 500 THEN 'High' ELSEIF [sales] >= 100 THEN 'Medium' ELSE 'Low' END"
    )
    print(f"✅ [14] add_calculation (订单等级): {calc2_id[:8]}...")
    
    # ========================================
    # 15. 表达式筛选：只保留盈利订单
    # ========================================
    filter_id = builder.add_filter(
        name="Filter_Profitable",
        parent_id=calc2_id,
        expression="[profit] > 0"
    )
    print(f"✅ [15] add_filter (盈利订单): {filter_id[:8]}...")
    
    # ========================================
    # 16. 移除ID列
    # ========================================
    remove_id = builder.add_remove_columns(
        name="Remove_IDs",
        parent_id=filter_id,
        columns=["order_id", "city", "state"]
    )
    print(f"✅ [16] add_remove_columns: {remove_id[:8]}...")
    
    # ========================================
    # 17. 清理步骤
    # ========================================
    clean_id = builder.add_clean_step(
        name="Data_Validation",
        parent_id=remove_id
    )
    print(f"✅ [17] add_clean_step: {clean_id[:8]}...")
    
    # ========================================
    # 18. 聚合：按区域月度汇总
    # ========================================
    agg_id = builder.add_aggregate(
        name="Aggregate_By_Region",
        parent_id=clean_id,
        group_by=["Region", "Manager", "Month"],
        aggregations=[
            {"field": "sales", "function": "SUM", "output_name": "Total_Sales"},
            {"field": "profit", "function": "SUM", "output_name": "Total_Profit"},
            {"field": "quantity", "function": "COUNT", "output_name": "Order_Count"},
            {"field": "sales", "function": "AVG", "output_name": "Avg_Order_Value"}
        ]
    )
    print(f"✅ [18] add_aggregate (区域汇总): {agg_id[:8]}...")
    
    # ========================================
    # 19. 行转列：月度对比
    # ========================================
    pivot_id = builder.add_pivot(
        name="Pivot_Monthly",
        parent_id=agg_id,
        pivot_column="Month",
        aggregate_column="Total_Sales",
        new_columns=["2024-01", "2024-02"],
        group_by=["Region", "Manager"],
        aggregation="SUM"
    )
    print(f"✅ [19] add_pivot (月度对比): {pivot_id[:8]}...")
    
    # ========================================
    # 20. 列转行
    # ========================================
    unpivot_id = builder.add_unpivot(
        name="Unpivot_Months",
        parent_id=pivot_id,
        columns_to_unpivot=["2024-01", "2024-02"],
        name_column="Month",
        value_column="Sales"
    )
    print(f"✅ [20] add_unpivot (指标透视): {unpivot_id[:8]}...")
    
    # ========================================
    # 21. 输出
    # ========================================
    output_id = builder.add_output_server(
        name="Sales_Analysis_Output",
        parent_id=unpivot_id,
        datasource_name="Superstore_Sales_Analysis"
    )
    print(f"✅ [21] add_output_server: {output_id[:8]}...")
    
    # ========================================
    # 构建和保存
    # ========================================
    print()
    print("-" * 60)
    flow, display, meta = builder.build()
    print("✅ 构建完成")
    
    output_folder = "./demo_output/comprehensive"
    output_tfl = "./demo_output/comprehensive.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"✅ 已生成: {output_tfl}")
    print()
    print("=" * 60)
    print("功能覆盖总结")
    print("=" * 60)
    print(f"  • 输入节点: 5 个")
    print(f"    - add_input_sql: 1月/2月订单（带 WHERE 条件）")
    print(f"    - add_input_table: 客户/区域/退货（原表连接）")
    print(f"  • 并集节点: 1 个")
    print(f"  • 联接节点: 2 个")
    print(f"  • 清理节点: 7 个 (筛选/保留/重命名/计算/移除)")
    print(f"  • 聚合节点: 1 个")
    print(f"  • 转置节点: 2 个 (Pivot + Unpivot)")
    print(f"  • 输出节点: 1 个")
    print()
    print("下一步:")
    print("  1. 用 Tableau Prep 打开 demo_output/comprehensive.tfl")
    print("  2. 配置数据库连接 (localhost:3306, root, 无密码)")
    print("  3. 运行流程验证结果")
    print("=" * 60)


if __name__ == "__main__":
    run_comprehensive_demo()
