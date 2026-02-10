"""
cwprep 聚合与转置功能演示

业务场景：区域月度销售对比分析
- 合并1月和2月订单（Union）
- 按区域汇总销售指标（Aggregate）
- 月度对比透视（Pivot/Unpivot）

功能覆盖：
1. add_union - 并集合并
2. add_aggregate - 聚合统计
3. add_pivot - 行转列
4. add_unpivot - 列转行

使用方法：
    python examples/demo_aggregation.py
"""

from cwprep import TFLBuilder, TFLPackager

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_aggregation_demo():
    print("=" * 50)
    print("cwprep 聚合与转置功能演示")
    print("=" * 50)
    print()
    
    builder = TFLBuilder(flow_name="Regional Sales Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"✅ 添加数据库连接")
    
    # 1. 添加1月订单
    jan_id = builder.add_input_sql(
        name="Orders Jan 2024",
        sql="""
        SELECT 
            '2024-01' AS month_label,
            r.region_name,
            r.manager_name,
            o.sales,
            o.profit
        FROM orders o
        JOIN regions r ON o.region_id = r.region_id
        WHERE o.order_date >= '2024-01-01' 
          AND o.order_date < '2024-02-01'
        """,
        connection_id=conn_id
    )
    print(f"✅ [1] add_input_sql: 1月订单")
    
    # 2. 添加2月订单
    feb_id = builder.add_input_sql(
        name="Orders Feb 2024",
        sql="""
        SELECT 
            '2024-02' AS month_label,
            r.region_name,
            r.manager_name,
            o.sales,
            o.profit
        FROM orders o
        JOIN regions r ON o.region_id = r.region_id
        WHERE o.order_date >= '2024-02-01' 
          AND o.order_date < '2024-03-01'
        """,
        connection_id=conn_id
    )
    print(f"✅ [2] add_input_sql: 2月订单")
    
    # 3. 并集合并
    union_id = builder.add_union(
        name="Union Jan + Feb",
        parent_ids=[jan_id, feb_id]
    )
    print(f"✅ [3] add_union: 合并1-2月订单")
    
    # 4. 按区域和月份聚合
    agg_id = builder.add_aggregate(
        name="Aggregate by Region",
        parent_id=union_id,
        group_by=["region_name", "manager_name", "month_label"],
        aggregations=[
            {"field": "sales", "function": "SUM", "output_name": "total_sales"},
            {"field": "sales", "function": "COUNT", "output_name": "order_count"},
            {"field": "sales", "function": "AVG", "output_name": "avg_order_value"},
            {"field": "profit", "function": "SUM", "output_name": "total_profit"}
        ]
    )
    print(f"✅ [4] add_aggregate: 区域月度汇总 (SUM/COUNT/AVG)")
    
    # 5. 重命名字段
    rename_id = builder.add_rename(
        parent_id=agg_id,
        renames={
            "region_name": "Region",
            "manager_name": "Manager",
            "month_label": "Month"
        }
    )
    print(f"✅ [5] add_rename: 重命名字段")
    
    # 6. 行转列：月份对比
    pivot_id = builder.add_pivot(
        name="Pivot by Month",
        parent_id=rename_id,
        pivot_column="Month",
        aggregate_column="total_sales",
        new_columns=["2024-01", "2024-02"],
        group_by=["Region", "Manager"],
        aggregation="SUM"
    )
    print(f"✅ [6] add_pivot: 行转列 (Month → Columns)")
    
    # 7. 列转行
    unpivot_id = builder.add_unpivot(
        name="Unpivot Months",
        parent_id=pivot_id,
        columns_to_unpivot=["2024-01", "2024-02"],
        name_column="Month",
        value_column="Sales"
    )
    print(f"✅ [7] add_unpivot: 列转行")
    
    # 输出
    output_id = builder.add_output_server(
        name="Output",
        parent_id=unpivot_id,
        datasource_name="Regional_Sales_Report"
    )
    print(f"✅ [8] add_output_server: 添加输出")
    
    # 构建
    print()
    flow, display, meta = builder.build()
    
    output_folder = "./demo_output/aggregation"
    output_tfl = "./demo_output/aggregation.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"✅ 已生成: {output_tfl}")
    print()
    print("功能覆盖:")
    print("  • add_union: 合并多月数据")
    print("  • add_aggregate: SUM/COUNT/AVG")
    print("  • add_pivot: 行转列")
    print("  • add_unpivot: 列转行")


if __name__ == "__main__":
    run_aggregation_demo()
