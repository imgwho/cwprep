"""
测试 SDK 计算字段功能

使用方法：
    cd c:/Users/imgwho/Desktop/projects/20260203-tflgenerator
    python test_calculation.py
"""

from core.builder import TFLBuilder
from core.packager import TFLPackager

def test_calculation():
    print("=== 测试计算字段功能 ===\n")
    
    builder = TFLBuilder(flow_name="测试计算字段")
    conn_id = builder.add_connection_from_config()
    print(f"✅ 添加连接: {conn_id[:8]}...")
    
    # 订单表
    input_id = builder.add_input_sql(
        name="订单表",
        sql="""
        SELECT 
            id AS 订单ID,
            total_price / 1000000 AS 订单营业额,
            DATE(FROM_UNIXTIME(check_time)) AS 下单日期
        FROM e_product_order_list 
        WHERE trash = 0 
        LIMIT 1000
        """,
        connection_id=conn_id
    )
    print(f"✅ 添加输入: {input_id[:8]}...")
    
    # 测试1: 计算字段 - 条件判断
    calc_id = builder.add_calculation(
        name="计算有效天",
        parent_id=input_id,
        column_name="是否有效天",
        formula="IF [订单营业额] > 99 THEN 1 ELSE 0 END"
    )
    print(f"✅ 添加计算字段(条件): {calc_id[:8]}...")
    
    # 测试2: 计算字段 - 日期提取
    calc2_id = builder.add_calculation(
        name="提取年份",
        parent_id=calc_id,
        column_name="年份",
        formula="DATEPART('year', [下单日期])"
    )
    print(f"✅ 添加计算字段(日期): {calc2_id[:8]}...")
    
    # 测试3: 计算字段 - 数学运算
    calc3_id = builder.add_calculation(
        name="计算订单等级",
        parent_id=calc2_id,
        column_name="订单等级",
        formula="IF [订单营业额] >= 1000 THEN '大单' ELSEIF [订单营业额] >= 100 THEN '中单' ELSE '小单' END"
    )
    print(f"✅ 添加计算字段(多条件): {calc3_id[:8]}...")
    
    # 聚合
    agg_id = builder.add_aggregate(
        name="按等级汇总",
        parent_id=calc3_id,
        group_by=["订单等级"],
        aggregations=[
            {"field": "订单营业额", "function": "SUM", "output_name": "总金额"},
            {"field": "是否有效天", "function": "SUM", "output_name": "有效天数"},
            {"field": "订单ID", "function": "COUNT", "output_name": "订单数"}
        ]
    )
    print(f"✅ 添加聚合: {agg_id[:8]}...")
    
    # 输出
    output_id = builder.add_output_server(
        name="输出",
        parent_id=agg_id,
        datasource_name="测试计算字段"
    )
    print(f"✅ 添加输出: {output_id[:8]}...")
    
    # 构建
    flow, display, meta = builder.build()
    print("\n✅ 构建完成")
    
    # 保存
    output_folder = "workspace/output/测试计算字段"
    output_tfl = "workspace/output/测试计算字段.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"\n✅ 已生成: {output_tfl}")
    print("\n验证方法: 用 Tableau Prep 打开上述 .tfl 文件")

if __name__ == "__main__":
    test_calculation()
