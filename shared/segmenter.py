# shared/segmenter.py
import math
from typing import Any, Dict, List

def segment_utterances(
    utterances: List[Dict[str, Any]],
    segment_ms: int = 30_000
) -> List[Dict[str, Any]]:
    """
    utterances: [{start_ms, end_ms, text}]
    returns: [{segment_id, start_ms, end_ms, text}]
    """
    if not utterances:
        return []

    max_end = max(u.get("end_ms", 0) for u in utterances)
    num_segments = int(math.ceil(max_end / segment_ms))

    segments: List[Dict[str, Any]] = []
    u_idx = 0

    for i in range(num_segments):
        start = i * segment_ms
        end = start + segment_ms

        # advance to first utterance that could overlap
        while u_idx < len(utterances) and utterances[u_idx].get("end_ms", 0) <= start:
            u_idx += 1

        texts = []
        j = u_idx
        while j < len(utterances):
            u = utterances[j]
            u_start = u.get("start_ms", 0)
            u_end = u.get("end_ms", 0)
            if u_start >= end:
                break
            if u_end > start and u_start < end:
                t = (u.get("text") or "").strip()
                if t:
                    texts.append(t)
            j += 1

        segments.append({
            "segment_id": f"{i:04d}",
            "start_ms": start,
            "end_ms": end,
            "text": " ".join(texts).strip(),
        })

    return segments
