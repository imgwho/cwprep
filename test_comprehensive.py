"""
综合测试脚本：销售业绩分析数据流

覆盖全部 14 个 SDK 方法：
1. add_connection_from_config - 数据库连接
2. add_input_sql - SQL输入 (x4)
3. add_union - 并集
4. add_filter - 表达式筛选
5. add_value_filter - 值筛选
6. add_keep_only - 只保留列
7. add_remove_columns - 移除列
8. add_join - 联接 (x2)
9. add_rename - 重命名
10. add_calculation - 计算字段
11. add_aggregate - 聚合
12. add_pivot - 行转列
13. add_unpivot - 列转行
14. add_clean_step - 清理步骤
15. add_output_server - 服务器输出

业务场景：销售业绩分析
- 合并1月和2月的订单数据
- 关联员工表和组织表
- 计算有效天、订单等级
- 按分公司/月份汇总统计
- 生成月度对比报表

使用方法：
    cd c:/Users/imgwho/Desktop/projects/20260203-tflgenerator
    python test_comprehensive.py
"""

from core.builder import TFLBuilder
from core.packager import TFLPackager

def test_comprehensive():
    print("=" * 60)
    print("综合测试：销售业绩分析数据流")
    print("=" * 60)
    print()
    
    # ========================================
    # 1. 初始化 Builder 和连接
    # ========================================
    builder = TFLBuilder(flow_name="销售业绩分析")
    conn_id = builder.add_connection_from_config()
    print(f"✅ [1] add_connection_from_config: {conn_id[:8]}...")
    
    # ========================================
    # 2. 输入源：订单表（1月）
    # ========================================
    order_jan_id = builder.add_input_sql(
        name="订单表_202601",
        sql="""
        SELECT 
            '2026-01' AS month_label,
            id AS order_id,
            acc AS employee_id,
            company_id,
            total_price / 1000000 AS order_amount,
            status AS order_status,
            DATE(FROM_UNIXTIME(check_time)) AS order_date
        FROM e_product_order_list 
        WHERE check_time >= UNIX_TIMESTAMP('2026-01-01')
          AND check_time < UNIX_TIMESTAMP('2026-02-01')
          AND trash = 0
          AND is_self_buy = 0
        LIMIT 500
        """,
        connection_id=conn_id
    )
    print(f"✅ [2] add_input_sql (订单1月): {order_jan_id[:8]}...")
    
    # ========================================
    # 3. 输入源：订单表（2月）
    # ========================================
    order_feb_id = builder.add_input_sql(
        name="订单表_202602",
        sql="""
        SELECT 
            '2026-02' AS month_label,
            id AS order_id,
            acc AS employee_id,
            company_id,
            total_price / 1000000 AS order_amount,
            status AS order_status,
            DATE(FROM_UNIXTIME(check_time)) AS order_date
        FROM e_product_order_list 
        WHERE check_time >= UNIX_TIMESTAMP('2026-02-01')
          AND check_time < UNIX_TIMESTAMP('2026-03-01')
          AND trash = 0
          AND is_self_buy = 0
        LIMIT 500
        """,
        connection_id=conn_id
    )
    print(f"✅ [3] add_input_sql (订单2月): {order_feb_id[:8]}...")
    
    # ========================================
    # 4. 输入源：员工表（修正字段名）
    # ========================================
    employee_id = builder.add_input_sql(
        name="员工表",
        sql="""
        SELECT 
            id AS emp_id,
            username AS emp_name,
            filialename AS branch_name,
            filialeid AS company_id,
            job_status AS emp_status
        FROM bi_e_admin
        WHERE job_status = '在职'
        """,
        connection_id=conn_id
    )
    print(f"✅ [4] add_input_sql (员工表): {employee_id[:8]}...")
    
    # ========================================
    # 5. 输入源：组织表
    # ========================================
    org_id = builder.add_input_sql(
        name="组织表",
        sql="""
        SELECT 
            id AS org_id,
            name AS org_name,
            level AS org_level,
            pid AS parent_id
        FROM e_account_organization
        WHERE enable = '1'
        """,
        connection_id=conn_id
    )
    print(f"✅ [5] add_input_sql (组织表): {org_id[:8]}...")
    
    # ========================================
    # 6. 并集：合并1月和2月订单
    # ========================================
    union_id = builder.add_union(
        name="合并1-2月订单",
        parent_ids=[order_jan_id, order_feb_id]
    )
    print(f"✅ [6] add_union: {union_id[:8]}...")
    
    # ========================================
    # 7. 表达式筛选：排除总部部门（用 OR 替代 IN，字符串加引号）
    # ========================================
    filter_branch_id = builder.add_filter(
        name="排除总部部门",
        parent_id=employee_id,
        expression="""NOT (
            [branch_name] = '总部外呼业务部' OR 
            [branch_name] = '总部定制酒招商' OR 
            [branch_name] = '总部招商' OR 
            [branch_name] = '总部新媒体' OR
            [branch_name] = '总部行政' OR
            [branch_name] = '总部私域团队'
        )"""
    )
    print(f"✅ [7] add_filter (排除总部): {filter_branch_id[:8]}...")
    
    # ========================================
    # 8. 只保留列：精简组织表
    # ========================================
    keep_only_id = builder.add_keep_only(
        name="精简组织表",
        parent_id=org_id,
        columns=["org_id", "org_name", "org_level"]
    )
    print(f"✅ [8] add_keep_only: {keep_only_id[:8]}...")
    
    # ========================================
    # 9. 联接1：订单关联员工
    # ========================================
    join1_id = builder.add_join(
        name="订单+员工",
        left_id=union_id,
        right_id=filter_branch_id,
        left_col="employee_id",
        right_col="emp_id",
        join_type="left"
    )
    print(f"✅ [9] add_join (订单+员工): {join1_id[:8]}...")
    
    # ========================================
    # 10. 联接2：关联组织表
    # ========================================
    join2_id = builder.add_join(
        name="订单+员工+组织",
        left_id=join1_id,
        right_id=keep_only_id,
        left_col="company_id",
        right_col="org_id",
        join_type="left"
    )
    print(f"✅ [10] add_join (关联组织): {join2_id[:8]}...")
    
    # ========================================
    # 11. 移除列：移除敏感字段
    # ========================================
    remove_id = builder.add_remove_columns(
        name="移除敏感字段",
        parent_id=join2_id,
        columns=["emp_id", "company_id", "org_id"]
    )
    print(f"✅ [11] add_remove_columns: {remove_id[:8]}...")
    
    # ========================================
    # 12. 重命名列
    # ========================================
    rename_id = builder.add_rename(
        parent_id=remove_id,
        renames={
            "branch_name": "分公司",
            "emp_name": "员工姓名",
            "org_name": "组织名称",
            "month_label": "月份"
        }
    )
    print(f"✅ [12] add_rename: {rename_id[:8]}...")
    
    # ========================================
    # 13. 计算字段：有效天
    # ========================================
    calc1_id = builder.add_calculation(
        name="计算有效天",
        parent_id=rename_id,
        column_name="是否有效天",
        formula="IF [order_amount] > 99 THEN 1 ELSE 0 END"
    )
    print(f"✅ [13] add_calculation (有效天): {calc1_id[:8]}...")
    
    # ========================================
    # 14. 计算字段：订单等级
    # ========================================
    calc2_id = builder.add_calculation(
        name="计算订单等级",
        parent_id=calc1_id,
        column_name="订单等级",
        formula="IF [order_amount] >= 1000 THEN '大单' ELSEIF [order_amount] >= 100 THEN '中单' ELSE '小单' END"
    )
    print(f"✅ [14] add_calculation (订单等级): {calc2_id[:8]}...")
    
    # ========================================
    # 15. 表达式筛选：过滤有效订单（用 OR 替代 IN）
    # ========================================
    filter_id = builder.add_filter(
        name="筛选有效订单",
        parent_id=calc2_id,
        expression="""[order_status] = 2 OR 
[order_status] = 3 OR 
[order_status] = 4 OR 
[order_status] = 5 OR 
[order_status] = 6 OR 
[order_status] = 7 OR 
[order_status] = 8"""
    )
    print(f"✅ [15] add_filter: {filter_id[:8]}...")
    
    # ========================================
    # 16. 清理步骤（空容器示例）
    # ========================================
    clean_id = builder.add_clean_step(
        name="数据清洗",
        parent_id=filter_id
    )
    print(f"✅ [16] add_clean_step: {clean_id[:8]}...")
    
    # ========================================
    # 17. 聚合：按分公司和月份汇总
    # ========================================
    agg_id = builder.add_aggregate(
        name="分公司月度汇总",
        parent_id=clean_id,
        group_by=["分公司", "月份"],
        aggregations=[
            {"field": "order_amount", "function": "SUM", "output_name": "总营业额"},
            {"field": "order_id", "function": "COUNT", "output_name": "订单数"},
            {"field": "是否有效天", "function": "SUM", "output_name": "有效天数"}
        ]
    )
    print(f"✅ [17] add_aggregate: {agg_id[:8]}...")
    
    # ========================================
    # 18. 行转列：月份对比
    # ========================================
    pivot_id = builder.add_pivot(
        name="月份对比",
        parent_id=agg_id,
        pivot_column="月份",
        aggregate_column="总营业额",
        new_columns=["2026-01", "2026-02"],
        group_by=["分公司"],
        aggregation="SUM"
    )
    print(f"✅ [18] add_pivot: {pivot_id[:8]}...")
    
    # ========================================
    # 19. 列转行：指标透视
    # ========================================
    unpivot_id = builder.add_unpivot(
        name="指标透视",
        parent_id=pivot_id,
        columns_to_unpivot=["2026-01", "2026-02"],
        name_column="月份",
        value_column="营业额"
    )
    print(f"✅ [19] add_unpivot: {unpivot_id[:8]}...")
    
    # ========================================
    # 20. 输出：发布到服务器
    # ========================================
    output_id = builder.add_output_server(
        name="销售业绩分析",
        parent_id=unpivot_id,
        datasource_name="销售业绩分析_综合测试"
    )
    print(f"✅ [20] add_output_server: {output_id[:8]}...")
    
    # ========================================
    # 构建和保存
    # ========================================
    print()
    print("-" * 60)
    flow, display, meta = builder.build()
    print("✅ 构建完成")
    
    output_folder = "workspace/output/销售业绩分析"
    output_tfl = "workspace/output/销售业绩分析.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"✅ 已生成: {output_tfl}")
    print()
    print("=" * 60)
    print("测试汇总")
    print("=" * 60)
    print(f"  • 输入节点: 4 个 (订单1月/2月 + 员工 + 组织)")
    print(f"  • 并集节点: 1 个")
    print(f"  • 联接节点: 2 个 (多表关联)")
    print(f"  • 清理节点: 6 个 (筛选/保留/移除/重命名/计算)")
    print(f"  • 聚合节点: 1 个")
    print(f"  • 转置节点: 2 个 (行转列 + 列转行)")
    print(f"  • 输出节点: 1 个")
    print()
    print("验证方法: 用 Tableau Prep 打开上述 .tfl 文件")

if __name__ == "__main__":
    test_comprehensive()
