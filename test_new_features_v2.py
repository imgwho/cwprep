"""
测试 SDK 第二批新功能：移除列、值筛选

使用方法：
    cd c:/Users/imgwho/Desktop/projects/20260203-tflgenerator
    python test_new_features_v2.py
"""

from core.builder import TFLBuilder
from core.packager import TFLPackager

def test_new_features_v2():
    print("=== 测试 SDK 新功能 V2 ===\n")
    
    builder = TFLBuilder(flow_name="测试新功能V2")
    conn_id = builder.add_connection_from_config()
    print(f"✅ 添加连接: {conn_id[:8]}...")
    
    # 订单表
    input_id = builder.add_input_sql(
        name="订单表",
        sql="SELECT * FROM e_product_order_list WHERE trash = 0 LIMIT 1000",
        connection_id=conn_id
    )
    print(f"✅ 添加输入: {input_id[:8]}...")
    
    # 测试1: 移除不需要的列
    remove_id = builder.add_remove_columns(
        name="移除敏感列",
        parent_id=input_id,
        columns=["password", "salt", "token", "mobile"]
    )
    print(f"✅ 添加移除列: {remove_id[:8]}...")
    
    # 测试2: 值筛选 - 只保留特定值
    value_filter_id = builder.add_value_filter(
        name="筛选已完成订单",
        parent_id=remove_id,
        field="trash",
        values=["0"],
        exclude=False  # 只保留 trash=0 的记录
    )
    print(f"✅ 添加值筛选: {value_filter_id[:8]}...")
    
    # 测试3: 表达式筛选
    filter_id = builder.add_filter(
        name="筛选有效金额",
        parent_id=value_filter_id,
        expression='[total_price] > 0'
    )
    print(f"✅ 添加表达式筛选: {filter_id[:8]}...")
    
    # 输出
    output_id = builder.add_output_server(
        name="输出",
        parent_id=filter_id,
        datasource_name="测试新功能V2"
    )
    print(f"✅ 添加输出: {output_id[:8]}...")
    
    # 构建
    flow, display, meta = builder.build()
    print("\n✅ 构建完成")
    
    # 保存
    output_folder = "workspace/output/测试新功能V2"
    output_tfl = "workspace/output/测试新功能V2.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"\n✅ 已生成: {output_tfl}")
    print("\n验证方法: 用 Tableau Prep 打开上述 .tfl 文件")

if __name__ == "__main__":
    test_new_features_v2()
