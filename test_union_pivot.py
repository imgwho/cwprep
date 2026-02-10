"""
测试 SDK 并集和转置功能

使用方法：
    cd c:/Users/imgwho/Desktop/projects/20260203-tflgenerator
    python test_union_pivot.py
"""

from core.builder import TFLBuilder
from core.packager import TFLPackager

def test_union_pivot():
    print("=== 测试并集和转置功能 ===\n")
    
    builder = TFLBuilder(flow_name="测试并集和转置")
    conn_id = builder.add_connection_from_config()
    print(f"✅ 添加连接: {conn_id[:8]}...")
    
    # 输入1: 2026年1月数据
    input1_id = builder.add_input_sql(
        name="消息表_202601",
        sql="""
        SELECT 
            '2026-01' AS month_label,
            id,
            UserName AS staff_wechat_id,
            Talker AS customer_wechat_id,
            HOUR(FROM_UNIXTIME(`Time`)) AS hour_of_day
        FROM e_vdata_message_202601
        LIMIT 100
        """,
        connection_id=conn_id
    )
    print(f"✅ 添加输入1(1月): {input1_id[:8]}...")
    
    # 输入2: 2026年2月数据
    input2_id = builder.add_input_sql(
        name="消息表_202602",
        sql="""
        SELECT 
            '2026-02' AS month_label,
            id,
            UserName AS staff_wechat_id,
            Talker AS customer_wechat_id,
            HOUR(FROM_UNIXTIME(`Time`)) AS hour_of_day
        FROM e_vdata_message_202602
        LIMIT 100
        """,
        connection_id=conn_id
    )
    print(f"✅ 添加输入2(2月): {input2_id[:8]}...")
    
    # 测试1: 并集
    union_id = builder.add_union(
        name="合并1月2月数据",
        parent_ids=[input1_id, input2_id]
    )
    print(f"✅ 添加并集: {union_id[:8]}...")
    
    # 测试2: 行转列（Pivot）- 按月份统计
    pivot_id = builder.add_pivot(
        name="按月份统计消息数",
        parent_id=union_id,
        pivot_column="month_label",
        aggregate_column="id",
        new_columns=["2026-01", "2026-02"],
        aggregation="COUNT"
    )
    print(f"✅ 添加行转列: {pivot_id[:8]}...")
    
    # 测试3: 列转行（Unpivot）
    unpivot_id = builder.add_unpivot(
        name="微信ID列转行",
        parent_id=pivot_id,
        columns_to_unpivot=["2026-01", "2026-02"],
        name_column="月份",
        value_column="消息数"
    )
    print(f"✅ 添加列转行: {unpivot_id[:8]}...")
    
    # 输出
    output_id = builder.add_output_server(
        name="输出",
        parent_id=unpivot_id,
        datasource_name="测试并集和转置"
    )
    print(f"✅ 添加输出: {output_id[:8]}...")
    
    # 构建
    flow, display, meta = builder.build()
    print("\n✅ 构建完成")
    
    # 保存
    output_folder = "workspace/output/测试并集和转置"
    output_tfl = "workspace/output/测试并集和转置.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"\n✅ 已生成: {output_tfl}")
    print("\n验证方法: 用 Tableau Prep 打开上述 .tfl 文件")

if __name__ == "__main__":
    test_union_pivot()
