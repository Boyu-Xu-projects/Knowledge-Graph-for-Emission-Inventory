import pandas as pd
import json
import os
from collections import defaultdict

# ========= 配置 =========
INPUT_FILE = r"C:\Users\xuboy\saf_llm_ner_project\FCC-organise-entities\03-14-FCC-data.xlsx"
OUTPUT_JSON = r"C:\Users\xuboy\saf_llm_ner_project\FCC-KG\graph.json"
# ======================

print(f"📂 Reading file: {os.path.basename(INPUT_FILE)}")

# 读取Excel文件
df = pd.read_excel(INPUT_FILE)

print("✅ Total rows loaded:", len(df))

nodes = {}  # key: (name, type) -> node
links = []
node_counter = 0  # 用于生成唯一ID

def clean_str(x):
    if pd.isna(x) or x is None:
        return None
    return str(x).strip()

# 遍历每一行
for idx, row in df.iterrows():
    # head 相关信息
    head = clean_str(row.get("head"))
    head_type = clean_str(row.get("head_type"))
    
    # tail 相关信息  
    tail = clean_str(row.get("tail"))
    tail_type = clean_str(row.get("tail_type"))
    
    # 关系
    relation = clean_str(row.get("relation"))
    
    # 文献信息
    file = clean_str(row.get("file"))
    title = clean_str(row.get("title"))
    doi = clean_str(row.get("doi"))
    year = clean_str(row.get("year"))

    if not head or not tail:
        continue

    # ---- head node ----
    # head_type 描述的是 head 这个实体的类型
    if head_type:  
        head_key = (head, head_type)
        if head_key not in nodes:
            nodes[head_key] = {
                "id": node_counter,
                "name": head,
                "label": head,
                "type": head_type,  # head 节点的类型
                "files": set(),
                "titles": set(),
                "dois": set(),
                "years": set()
            }
            node_counter += 1

        # 添加文献信息到 head 节点
        if file:  nodes[head_key]["files"].add(file)
        if title: nodes[head_key]["titles"].add(title)
        if doi:   nodes[head_key]["dois"].add(doi)
        if year:  nodes[head_key]["years"].add(year)

        head_node_id = nodes[head_key]["id"]
    else:
        continue

    # ---- tail node ----
    # tail_type 描述的是 tail 这个实体的类型
    if tail_type:
        tail_key = (tail, tail_type)
        if tail_key not in nodes:
            nodes[tail_key] = {
                "id": node_counter,
                "name": tail,
                "label": tail,
                "type": tail_type,  # tail 节点的类型
                "files": set(),
                "titles": set(),
                "dois": set(),
                "years": set()
            }
            node_counter += 1

        # 添加文献信息到 tail 节点
        if file:  nodes[tail_key]["files"].add(file)
        if title: nodes[tail_key]["titles"].add(title)
        if doi:   nodes[tail_key]["dois"].add(doi)
        if year:  nodes[tail_key]["years"].add(year)

        tail_node_id = nodes[tail_key]["id"]
    else:
        continue

    # ---- edge ----
    # 创建 head 和 tail 之间的关系
    if relation:
        links.append({
            "source": head_node_id,
            "target": tail_node_id,
            "relation": relation
        })

# 将set转换为list
for node in nodes.values():
    node["files"] = list(node["files"])
    node["titles"] = list(node["titles"])
    node["dois"] = list(node["dois"])
    node["years"] = list(node["years"])
    node["paper_count"] = len(node["titles"])

# 构建最终的图数据
graph = {
    "nodes": list(nodes.values()),
    "links": links
}

# 确保输出目录存在
os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

# 保存JSON
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(graph, f, indent=2, ensure_ascii=False)

print(f"\n🎉 graph.json created successfully!")
print(f"   Nodes: {len(graph['nodes'])}")
print(f"   Links: {len(graph['links'])}")
print(f"   Output: {OUTPUT_JSON}")

# 统计信息
print("\n📊 Node type statistics:")
type_counts = {}
for node in graph['nodes']:
    node_type = node['type']
    type_counts[node_type] = type_counts.get(node_type, 0) + 1

for node_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"   {node_type:30}: {count:4d}")

print("\n📊 Relation statistics:")
relation_counts = {}
for link in graph['links']:
    rel = link['relation']
    relation_counts[rel] = relation_counts.get(rel, 0) + 1

for rel, count in sorted(relation_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"   {rel:30}: {count:4d}")

# 可选：保存节点-文献关系表
node_papers = []
for node in graph['nodes']:
    for title in node.get('titles', []):
        node_papers.append({
            'node_id': node['id'],
            'node_name': node['name'],
            'node_type': node['type'],
            'paper_title': title
        })

if node_papers:
    papers_df = pd.DataFrame(node_papers)
    papers_output = os.path.join(os.path.dirname(OUTPUT_JSON), "node_papers.xlsx")
    papers_df.to_excel(papers_output, index=False)
    print(f"\n📄 Node-paper relations saved: {papers_output}")