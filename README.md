
# Azure Speech Batch Transcription Function

Small Azure Functions HTTP endpoint for **Azure Speech-to-Text Batch Transcription**. Used as part of the video-annotator project.


Workflow:

1. Provide a **Blob SAS URL** to an audio file (`media_url`)
2. Function submits a batch job and returns a `job_url`
3. Call again with `job_url` to get status; when `Succeeded`, it returns normalized transcript output.

## Repo layout

* `TranscribeHttp/`

  * `__init__.py` — HTTP function (submit + status/result)
  * `function.json` — HTTP trigger bindings
* `shared/speech_batch.py` — Speech + Blob helpers (submit job, list files, normalize output)
* `make_sas.py` — generate a SAS URL for a blob (for testing)
* `test_transcribe.py` — local test driver
* `local.settings.json` — local dev settings (do not commit secrets)
* `requirements.txt`, `host.json`

## Prereqs

* Python 3.11+
* Azure Functions Core Tools v4
* Azure Speech resource (`SPEECH_KEY`, region or endpoint)
* Azure Storage account with a container that holds your audio (defaults assume `speech-input`)

Install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Local config is read from `local.settings.json`:

* `SPEECH_KEY`
* `SPEECH_REGION` (optional if `SPEECH_ENDPOINT` is set)
* `SPEECH_ENDPOINT` (e.g. `https://eastus.api.cognitive.microsoft.com/`)
* `SPEECH_API_VERSION` (e.g. `2025-10-15`)
* Storage vars if you’re using the helper scripts:

  * `AZURE_STORAGE_ACCOUNT`
  * `AZURE_STORAGE_KEY`
  * `INPUT_CONTAINER` (default `speech-input`)
  * `OUTPUT_CONTAINER` (default `speech-output`)

## Run locally

Start the Functions host:

```bash
func start
```

### 1) Get a SAS URL for an audio blob

If you already uploaded an audio file to Blob Storage, run:

```bash
python make_sas.py
```

It prints a SAS URL you can use as `media_url`.

### 2) Submit a transcription job

```bash
curl -sS -X POST "http://localhost:7071/api/TranscribeHttp" \
  -H "Content-Type: application/json" \
  -d '{
    "media_url": "https://<account>.blob.core.windows.net/<container>/<file>.m4a?<sas>",
    "locale": "en-US",
    "display_name": "measles_short"
  }' | jq
```

Response (202):

```json
{ "job_url": "https://.../speechtotext/transcriptions/<id>?api-version=..." }
```

### 3) Check status / fetch transcript

```bash
curl -sS -X POST "http://localhost:7071/api/TranscribeHttp" \
  -H "Content-Type: application/json" \
  -d '{ "job_url": "PASTE_JOB_URL_HERE" }' | jq
```

Possible responses:

* Running:

```json
{ "status": "Running", "job_url": "..." }
```

* Failed:

```json
{ "status": "Failed", "job": { ... } }
```

* Succeeded:

```json
{ "status": "Succeeded", "result": { "utterances": [...], "words": [...] } }
```

## Notes

* Batch jobs may sit in `Running` due to queueing; poll until `Succeeded`/`Failed`.
* Normalization returns `utterances` and (if present) `words` with timestamps in milliseconds.
* The shared submit payload uses `channels: [0]` to avoid duplicated results from stereo audio.

## Deploy

Set the same configuration values as app settings in your Azure Function App, then publish:

```bash
func azure functionapp publish <APP_NAME> --python
```

