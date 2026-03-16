import pandas as pd
import json
import os

# ========= 配置 =========
INPUT_FILE = r"C:\Users\xuboy\saf_llm_ner_project\FCC-organise-entities\03-14-FCC-data.xlsx"
OUTPUT_JSON = r"C:\Users\xuboy\saf_llm_ner_project\FCC-KG\graph.json"
# ======================

print(f"📂 Reading file: {os.path.basename(INPUT_FILE)}")

df = pd.read_excel(INPUT_FILE)

print("✅ Total rows loaded:", len(df))

nodes = {}
links = []
node_counter = 0

link_set = set()


def clean_str(x):
    if pd.isna(x) or x is None:
        return None
    # 转换为字符串并去除前后空格
    str_val = str(x).strip()
    # 如果是数字格式的年份（如2023.0），去掉.0
    if str_val.endswith('.0') and str_val.replace('.0', '').isdigit():
        str_val = str_val.replace('.0', '')
    return str_val


# 首先，创建一个全局的论文映射，使用标题作为唯一标识
paper_map = {}  # key: title -> paper_info

for idx, row in df.iterrows():
    title = clean_str(row.get("title"))
    
    if title:  # 只要求标题存在，不要求文件存在
        # 如果标题已存在，合并信息（优先使用非空的DOI）
        if title in paper_map:
            existing = paper_map[title]
            new_doi = clean_str(row.get("doi"))
            new_year = clean_str(row.get("year"))
            new_file = clean_str(row.get("file"))
            
            # 如果现有DOI为空但有新的DOI，更新
            if not existing["doi"] and new_doi:
                existing["doi"] = new_doi
            
            # 收集所有相关的年份和文件（去重）
            if new_year and new_year not in existing["years"]:
                existing["years"].append(new_year)
            if new_file and new_file not in existing["files"]:
                existing["files"].append(new_file)
        else:
            # 新标题，创建论文信息
            paper_map[title] = {
                "title": title,
                "doi": clean_str(row.get("doi")),
                "years": [clean_str(row.get("year"))] if clean_str(row.get("year")) else [],
                "files": [clean_str(row.get("file"))] if clean_str(row.get("file")) else []
            }

print(f"📄 Found {len(paper_map)} unique papers based on title")

# 重新遍历数据，构建节点和边
for idx, row in df.iterrows():

    head = clean_str(row.get("head"))
    head_type = clean_str(row.get("head_type"))

    tail = clean_str(row.get("tail"))
    tail_type = clean_str(row.get("tail_type"))

    relation = clean_str(row.get("relation"))

    file = clean_str(row.get("file"))
    title = clean_str(row.get("title"))
    year = clean_str(row.get("year"))

    if not head or not tail:
        continue

    # ===== HEAD NODE =====
    head_key = (head, head_type)

    if head_key not in nodes:
        nodes[head_key] = {
            "id": node_counter,
            "name": head,
            "label": head,
            "type": head_type,
            "files": set(),
            "years": set(),
            "papers": [],          # 存储论文对象
            "paper_titles": set()   # 用于去重，基于标题
        }
        node_counter += 1

    head_node = nodes[head_key]

    if file:
        head_node["files"].add(file)

    if year:
        head_node["years"].add(year)

    # ===== PAPER 处理 =====
    if title:
        # 只添加不重复的论文（基于标题）
        if title not in head_node["paper_titles"]:
            # 从全局paper_map获取论文信息
            paper_info = paper_map.get(title, {
                "title": title,
                "doi": clean_str(row.get("doi")),
                "years": [year] if year else [],
                "files": [file] if file else []
            })
            
            head_node["papers"].append(paper_info)
            head_node["paper_titles"].add(title)

    head_node_id = head_node["id"]

    # ===== TAIL NODE =====
    tail_key = (tail, tail_type)

    if tail_key not in nodes:
        nodes[tail_key] = {
            "id": node_counter,
            "name": tail,
            "label": tail,
            "type": tail_type,
            "files": set(),
            "years": set(),
            "papers": [],
            "paper_titles": set()
        }
        node_counter += 1

    tail_node = nodes[tail_key]

    if file:
        tail_node["files"].add(file)

    if year:
        tail_node["years"].add(year)

    if title:
        if title not in tail_node["paper_titles"]:
            paper_info = paper_map.get(title, {
                "title": title,
                "doi": clean_str(row.get("doi")),
                "years": [year] if year else [],
                "files": [file] if file else []
            })
            
            tail_node["papers"].append(paper_info)
            tail_node["paper_titles"].add(title)

    tail_node_id = tail_node["id"]

    # ===== EDGE =====
    if relation:
        edge_key = (head_node_id, tail_node_id, relation)

        if edge_key not in link_set:
            links.append({
                "source": head_node_id,
                "target": tail_node_id,
                "relation": relation
            })
            link_set.add(edge_key)


# ===== FINALIZE NODE DATA =====
for node in nodes.values():
    # 转换set为list
    node["files"] = list(node["files"])
    node["years"] = list(node["years"])
    node["paper_count"] = len(node["papers"])
    
    # 简化papers结构，只保留必要的字段
    simplified_papers = []
    for paper in node["papers"]:
        # 获取第一个年份并确保没有.0
        first_year = ""
        if paper.get("years") and len(paper["years"]) > 0:
            first_year = paper["years"][0]
            # 再次确保年份没有.0
            if first_year and first_year.endswith('.0') and first_year.replace('.0', '').isdigit():
                first_year = first_year.replace('.0', '')
        
        simplified_paper = {
            "title": paper["title"],
            "doi": paper.get("doi", ""),
            "year": first_year,  # 使用处理后的年份
            "all_years": [y if not (y.endswith('.0') and y.replace('.0', '').isdigit()) else y.replace('.0', '') 
                         for y in paper.get("years", [])],
            "all_files": paper.get("files", [])
        }
        simplified_papers.append(simplified_paper)
    
    # 按标题排序
    simplified_papers.sort(key=lambda x: x["title"])
    
    node["papers"] = simplified_papers
    
    # 删除内部去重字段
    del node["paper_titles"]


graph = {
    "nodes": list(nodes.values()),
    "links": links
}

os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(graph, f, indent=2, ensure_ascii=False)


print(f"\n🎉 graph.json created successfully!")
print(f"   Nodes: {len(graph['nodes'])}")
print(f"   Links: {len(graph['links'])}")
print(f"   Output: {OUTPUT_JSON}")


# ===== STATISTICS =====
print("\n📊 Node type statistics:")
type_counts = {}
for node in graph["nodes"]:
    t = node["type"]
    type_counts[t] = type_counts.get(t, 0) + 1

for t, c in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"   {t:30}: {c:4d}")

print("\n📊 Relation statistics:")
rel_counts = {}
for link in graph["links"]:
    r = link["relation"]
    rel_counts[r] = rel_counts.get(r, 0) + 1

for r, c in sorted(rel_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"   {r:30}: {c:4d}")


# ===== EXPORT NODE-PAPER TABLE =====
node_papers = []
paper_stats = {"total_papers": 0, "papers_with_doi": 0}

for node in graph["nodes"]:
    for p in node.get("papers", []):
        node_papers.append({
            "node_id": node["id"],
            "node_name": node["name"],
            "node_type": node["type"],
            "title": p.get("title"),
            "doi": p.get("doi"),
            "year": p.get("year"),
            "all_years": ", ".join(p.get("all_years", [])),
            "all_files": ", ".join(p.get("all_files", []))
        })
        paper_stats["total_papers"] += 1
        if p.get("doi"):
            paper_stats["papers_with_doi"] += 1

if node_papers:
    papers_df = pd.DataFrame(node_papers)
    papers_output = os.path.join(
        os.path.dirname(OUTPUT_JSON),
        "node_papers.xlsx"
    )
    papers_df.to_excel(papers_output, index=False)
    
    print(f"\n📄 Node-paper relations saved: {papers_output}")
    print(f"   Total paper entries: {paper_stats['total_papers']}")
    print(f"   Papers with DOI: {paper_stats['papers_with_doi']} ({paper_stats['papers_with_doi']/paper_stats['total_papers']*100:.1f}%)")
    
    # 检查每个节点的论文是否有重复
    print("\n🔍 Checking for paper consistency:")
    for node in graph["nodes"]:
        if len(node.get("papers", [])) > 1:
            # 检查是否有重复的title
            titles = [p.get("title") for p in node["papers"]]
            if len(titles) != len(set(titles)):
                print(f"   ⚠️  Node {node['name']} ({node['type']}) has duplicate titles")
            
            # 检查是否有重复的DOI
            dois = [p.get("doi") for p in node["papers"] if p.get("doi")]
            if len(dois) != len(set(dois)):
                print(f"   ⚠️  Node {node['name']} ({node['type']}) has duplicate DOIs")