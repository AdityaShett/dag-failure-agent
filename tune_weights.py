
import itertools, json, os
from google.cloud import bigquery

PROJECT = os.environ["GCP_PROJECT"]
TABLE = f"{PROJECT}.dag_failure_agent.confidence_signals"
MIN_PER_CLASS = 5
STEP = 0.05

def fetch_rows():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        WITH latest_outcome AS (
          SELECT record_id, outcome,
                 ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS rn
          FROM `{PROJECT}.dag_failure_agent.confidence_outcomes`
        )
        SELECT s.s_llm, s.s_retrieval, s.s_history, s.s_logs, s.s_source,
               IF(COALESCE(o.outcome, s.outcome) = 'merged', 1, 0) AS merged
        FROM `{TABLE}` s
        LEFT JOIN latest_outcome o ON o.record_id = s.record_id AND o.rn = 1
        WHERE COALESCE(o.outcome, s.outcome) IN ('merged', 'rejected')
    """
    rows = [dict(r) for r in client.query(sql).result()]
    merged = sum(r["merged"] for r in rows)
    rejected = len(rows) - merged
    if merged < MIN_PER_CLASS or rejected < MIN_PER_CLASS:
        raise ValueError(f"Need >= {MIN_PER_CLASS}/class, got merged={merged} rejected={rejected}")
    return rows

def score(row, w):
    return (w["llm"]*row["s_llm"] + w["retrieval"]*row["s_retrieval"] +
            w["history"]*row["s_history"] + w["logs"]*row["s_logs"] +
            w["source"]*row["s_source"])

def accuracy(rows, w, threshold=0.75):
    correct = sum(1 for r in rows if (score(r, w) >= threshold) == bool(r["merged"]))
    return correct / len(rows)

def grid_search(rows):
    keys = ["llm", "retrieval", "history", "logs", "source"]
    best_w, best_acc = None, -1
    steps = [round(i*STEP, 2) for i in range(int(1/STEP)+1)]
    for combo in itertools.product(steps, repeat=len(keys)-1):
        if round(sum(combo), 4) > 1.0:
            continue
        last = round(1.0 - sum(combo), 4)
        w = dict(zip(keys, list(combo) + [last]))
        acc = accuracy(rows, w)
        if acc > best_acc:
            best_acc, best_w = acc, w
    return best_w, best_acc

if __name__ == "__main__":
    rows = fetch_rows()
    best_w, best_acc = grid_search(rows)
    print(f"Best weights: {best_w} (accuracy={best_acc:.3f})")
    with open("config/weights.json", "w") as f:
        json.dump(best_w, f, indent=2)