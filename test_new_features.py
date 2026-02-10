"""
测试 SDK 新功能：清理步骤、筛选、聚合

测试流程：
1. 从订单表读取数据
2. 只保留需要的列
3. 重命名列
4. 筛选数据
5. 聚合统计
6. 输出

使用方法：
    cd c:/Users/imgwho/Desktop/projects/20260203-tflgenerator
    python test_new_features.py
    
验证方法：
    用 Tableau Prep 打开 workspace/output/测试新功能.tfl
"""

from core.builder import TFLBuilder
from core.packager import TFLPackager

def test_new_features():
    print("=== 测试 SDK 新功能 ===\n")
    
    # 创建构建器
    builder = TFLBuilder(flow_name="测试新功能")
    
    # 添加数据库连接
    conn_id = builder.add_connection_from_config()
    print(f"✅ 添加连接: {conn_id[:8]}...")
    
    # 添加 SQL 输入
    input_id = builder.add_input_sql(
        name="订单表",
        sql="""
        SELECT 
            id AS 订单ID,
            user_id AS 客户ID,
            company_id AS 公司ID,
            total_price / 1000000 AS 订单金额,
            DATE(FROM_UNIXTIME(check_time)) AS 下单日期
        FROM e_product_order_list 
        WHERE trash = 0 
        LIMIT 1000
        """,
        connection_id=conn_id
    )
    print(f"✅ 添加输入: {input_id[:8]}...")
    
    # 测试1: 只保留列
    keep_id = builder.add_keep_only(
        name="只保留关键列",
        parent_id=input_id,
        columns=["订单ID", "客户ID", "公司ID", "订单金额", "下单日期"]
    )
    print(f"✅ 添加只保留列: {keep_id[:8]}...")
    
    # 测试2: 重命名列
    rename_id = builder.add_rename(
        parent_id=keep_id,
        renames={
            "公司ID": "分公司ID",
            "订单金额": "金额"
        }
    )
    print(f"✅ 添加重命名: {rename_id[:8]}...")
    
    # 测试3: 筛选
    filter_id = builder.add_filter(
        name="筛选有效订单",
        parent_id=rename_id,
        expression='[金额] > 0 AND NOT ISNULL([客户ID])'
    )
    print(f"✅ 添加筛选: {filter_id[:8]}...")
    
    # 测试4: 聚合
    agg_id = builder.add_aggregate(
        name="按公司汇总",
        parent_id=filter_id,
        group_by=["分公司ID"],
        aggregations=[
            {"field": "金额", "function": "SUM", "output_name": "总金额"},
            {"field": "订单ID", "function": "COUNT", "output_name": "订单数"}
        ]
    )
    print(f"✅ 添加聚合: {agg_id[:8]}...")
    
    # 添加输出
    output_id = builder.add_output_server(
        name="输出",
        parent_id=agg_id,
        datasource_name="测试新功能"
    )
    print(f"✅ 添加输出: {output_id[:8]}...")
    
    # 构建
    flow, display, meta = builder.build()
    print("\n✅ 构建完成")
    
    # 保存
    output_folder = "workspace/output/测试新功能"
    output_tfl = "workspace/output/测试新功能.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"\n✅ 已生成: {output_tfl}")
    print("\n验证方法: 用 Tableau Prep 打开上述 .tfl 文件")

if __name__ == "__main__":
    test_new_features()
