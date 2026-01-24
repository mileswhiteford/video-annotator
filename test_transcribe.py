import json
import  os
from shared.speech_batch import transcribe_and_normalize_from_local_file


def load_local_settings(path="local.settings.json"):
    if not os.path.exists(path):
        return
    data = json.load(open(path, "r", encoding="utf-8"))
    values = data.get("Values", {})
    for k, v in values.items():
        # don't overwrite real env vars if already set
        os.environ.setdefault(k, v)

if __name__ == "__main__":
    load_local_settings()

    out = transcribe_and_normalize_from_local_file(
        local_media_path="measles_short.m4a",
        locale="en-US",
    )

    print(json.dumps(out["utterances"][:5], indent=2))
    print(f"\nTotal utterances: {len(out['utterances'])}")

