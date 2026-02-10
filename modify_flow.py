import json
import os

# 流程文件路径
file_path = r'C:\Users\imgwho\Desktop\projects\20260203-tflgenerator\dataflow 18.订单表 客户\flow'

# 读取 JSON 内容
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# --- 定义需要删除的节点 ID ---
# 这些 ID 是通过阅读原始 flow 文件分析出来的
nodes_to_remove = [
    '63ccb08c-436d-4322-b30e-cb1f576ba017', # 输入节点: e_area_code (地理表)
    '5109d22c-86bf-4e63-b2cf-574938afe1d1', # 清理步骤: 清理 4 (重命名 code->地区code)
    'c8727462-2861-48f9-819f-725e3469fc8a', # 联接节点: 联接 4 (省份匹配)
    '67f8880b-7fea-4009-b23f-57bdcfb726ee', # 联接节点: 联接 6 (城市匹配)
    'e98277f5-03b3-477c-8601-847d8af79072'  # 联接节点: 联接 8 (区县匹配)
]

# --- 步骤 1: 删除节点定义 ---
# 从 "nodes" 字典中彻底移除这些节点
for node_id in nodes_to_remove:
    if node_id in data['nodes']:
        del data['nodes'][node_id]

# --- 步骤 2: 更新入口节点列表 ---
# 从 "initialNodes" 列表中移除 e_area_code 的 ID
data['initialNodes'] = [nid for nid in data['initialNodes'] if nid not in nodes_to_remove]

# --- 步骤 3: 删除节点属性 ---
# 如果这些节点有特定的属性配置（如主键设置），也一并移除
if 'nodeProperties' in data:
    for node_id in nodes_to_remove:
        if node_id in data['nodeProperties']:
            del data['nodeProperties'][node_id]

# --- 步骤 4: 核心步骤 - 重连线路 ---
# 我们切断了中间的环节，现在需要把断开的两头接上。
# 上游：订单表 (e_product_order_list)
# 下游：联接 3 (订单+员工)

order_node_id = 'e530fcad-aeea-4998-a61e-0bf1f1e1a223' # 订单表 ID
target_join_id = '46240063-aa4f-44c2-bb7c-89b910ddd1f9' # 下游联接节点 ID

if order_node_id in data['nodes']:
    # 修改订单表的 "nextNodes"，让它直接指向下游联接节点
    data['nodes'][order_node_id]['nextNodes'] = [
        {
            'namespace': 'Default',      # 输出命名空间通常是 Default
            'nextNodeId': target_join_id, # 指向新的目标
            'nextNamespace': 'Left'      # 重要：告诉下游节点，我连入的是它的"左侧"入口
        }
    ]

# 保存修改
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print('修改完成：已移除 e_area_code 及其相关联接步骤，并重连了数据流。')
