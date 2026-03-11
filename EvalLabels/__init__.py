"""
EvalLabels - Azure Function for evaluating labeling accuracy

Accepts a list of test cases (text + expected labels), runs them through
the same GPT labeler used by LabelSegments, and returns per-row results
and per-label accuracy metrics.

Input: POST with:
  {
    "test_cases": [
      {"text": "...", "expected_labels": ["Label1", "Label2"]},
      ...
    ]
  }

Output: JSON with:
  - rows: per-row comparison (expected, predicted, correct, missed, hallucinated)
  - metrics: per-label precision/recall/F1 + macro/micro F1

Environment Variables:
  AZURE_STORAGE_ACCOUNT   - Storage account name
  AZURE_STORAGE_KEY       - Storage account key
  LABELS_CONTAINER        - Blob container for label library (default: "labels")
  PROXY_BASE_URL          - Azure OpenAI proxy base URL
  FUNCTION_HOST_KEY       - Azure Function host key for the proxy
"""

import json
import logging
import os
from typing import Any, Dict, List

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from openai import OpenAI

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 20))

def _read_label_json() -> Dict[str, Any]:
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]
    service = BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )
    container = os.environ.get("LABELS_CONTAINER", "labels")
    bc = service.get_blob_client(container=container, blob="label_library.json")
    return json.loads(bc.download_blob().readall())


def _call_gpt(label_defs: List[Dict], seg_inputs: List[Dict]) -> List[Dict]:
    client = OpenAI(
        base_url=os.environ["PROXY_BASE_URL"],
        api_key="unused",
        default_headers={"x-functions-key": os.environ["FUNCTION_HOST_KEY"]},
    )

    system_prompt = (
        "You are a content labeler. Given label definitions and video transcript segments, "
        "decide whether each label applies to each segment. "
        "Return a decision for EVERY label for EVERY segment — including labels that do not apply. "
        'Your response must be a JSON object with a "results" key containing an array.'
    )

    user_prompt = (
        f"Labels:\n{json.dumps(label_defs, ensure_ascii=False)}\n\n"
        f"Segments:\n{json.dumps(seg_inputs, ensure_ascii=False)}\n\n"
        "For each segment, return every label with:\n"
        '  - "applied": true if the segment mentions, discusses, or is clearly related to the label topic\n'
        '  - "applied": false if the label does not apply\n'
        '  - "rationale": a brief explanation of your decision either way\n\n'
        "Return:\n"
        '{"results": [\n'
        '  {"segment_id": "0000", "labels": [\n'
        '    {"name": "LabelName", "applied": true, "rationale": "explanation"},\n'
        '    {"name": "OtherLabel", "applied": false, "rationale": "explanation"}\n'
        "  ]}\n"
        "]}"
    )

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        text={"format": {"type": "json_object"}},
    )

    return json.loads(resp.output_text).get("results", [])


def _compute_metrics(test_cases: List[Dict], predictions: List[List[str]]) -> Dict:
    # Collect all label names that appear in annotations
    all_labels = set()
    for tc in test_cases:
        all_labels.update(tc.get("expected_labels", []))

    per_label = {}
    for label in sorted(all_labels):
        tp = sum(
            1 for tc, pred in zip(test_cases, predictions)
            if label in tc["expected_labels"] and label in pred
        )
        fp = sum(
            1 for tc, pred in zip(test_cases, predictions)
            if label not in tc["expected_labels"] and label in pred
        )
        fn = sum(
            1 for tc, pred in zip(test_cases, predictions)
            if label in tc["expected_labels"] and label not in pred
        )
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        per_label[label] = {
            "tp": tp, "fp": fp, "fn": fn,
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1": round(f1, 3),
        }
    
    
    macro_f1 = sum(m["f1"] for m in per_label.values()) / len(per_label) if per_label else 0.0
    total_tp = sum(m["tp"] for m in per_label.values())
    total_fp = sum(m["fp"] for m in per_label.values())
    total_fn = sum(m["fn"] for m in per_label.values())
    micro_p = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0.0
    micro_r = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0.0
    micro_f1 = 2 * micro_p * micro_r / (micro_p + micro_r) if (micro_p + micro_r) > 0 else 0.0

    return {
        "per_label": per_label,
        "macro_f1": round(macro_f1, 3),
        "micro_f1": round(micro_f1, 3),
        "micro_precision": round(micro_p, 3),
        "micro_recall": round(micro_r, 3),
    }


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        test_cases = body.get("test_cases", [])

        if not test_cases:
            return func.HttpResponse(
                json.dumps({"error": "No test cases provided."}),
                mimetype="application/json",
                status_code=400,
            )

        library = _read_label_json()
        active_labels = [l for l in library.get("labels", []) if l.get("is_active", True)]
        label_defs = [{"name": l["name"], "description": l["description"]} for l in active_labels]
        valid_names = {l["name"] for l in active_labels}

        if not label_defs:
            return func.HttpResponse(
                json.dumps({"error": "No active labels found."}),
                mimetype="application/json",
                status_code=400,
            )

        # Call GPT in batches, using global index as segment_id
        results_map: Dict[str, Dict] = {}
        for i in range(0, len(test_cases), BATCH_SIZE):
            batch = test_cases[i: i + BATCH_SIZE]
            seg_inputs = [
                {"segment_id": str(i + j), "text": tc["text"]}
                for j, tc in enumerate(batch)
            ]
            try:
                gpt_results = _call_gpt(label_defs, seg_inputs)
                for r in gpt_results:
                    results_map[r["segment_id"]] = r
            except Exception as e:
                logging.warning(f"GPT batch {i} failed: {e}")

        # Build per-row results
        predictions: List[List[str]] = []
        rows = []
        unknown_labels: set = set()
        for idx, tc in enumerate(test_cases):
            result = results_map.get(str(idx), {})
            gpt_labels = result.get("labels", [])
            validated = [l for l in gpt_labels if isinstance(l, dict) and l.get("name") in valid_names]
            predicted = [l["name"] for l in validated if l.get("applied", False)]
            raw_expected = tc.get("expected_labels", [])
            row_unknown = [l for l in raw_expected if l not in valid_names]
            unknown_labels.update(row_unknown)
            expected = [l for l in raw_expected if l in valid_names]

            predictions.append(predicted)
            rows.append({
                "text": tc["text"],
                "expected": expected,
                "predicted": predicted,
                "correct": [l for l in predicted if l in expected],
                "missed": [l for l in expected if l not in predicted],
                "hallucinated": [l for l in predicted if l not in expected],
                "details": validated,
            })

        metrics = _compute_metrics(test_cases, predictions)

        return func.HttpResponse(
            json.dumps({"rows": rows, "metrics": metrics, "unknown_labels": list(unknown_labels)}, ensure_ascii=False),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.exception("EvalLabels failed")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
