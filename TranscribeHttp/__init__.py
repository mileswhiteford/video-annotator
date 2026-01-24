import json
import os
import azure.functions as func

from shared.speech_batch import (
    SpeechConfig,
    submit_transcription_job,
    get_job,
    get_transcript_urls,
    download_and_normalize,
)

def _cfg() -> SpeechConfig:
    endpoint = os.environ.get("SPEECH_ENDPOINT")
    if not endpoint:
        region = os.environ.get("SPEECH_REGION", "eastus")
        endpoint = f"https://{region}.api.cognitive.microsoft.com"
    return SpeechConfig(
        key=os.environ["SPEECH_KEY"],
        endpoint=endpoint.rstrip("/"),
        api_version=os.environ.get("SPEECH_API_VERSION", "2025-10-15"),
    )

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        cfg = _cfg()
        body = req.get_json()
        locale = body.get("locale", "en-US")

        # Mode 1: submit
        media_url = body.get("media_url")
        if media_url:
            job_url = submit_transcription_job(
                config=cfg,
                content_urls=[media_url],
                locale=locale,
                display_name=body.get("display_name") or "transcription",
                word_level_timestamps=True,
                ttl_hours=24,
            )
            return func.HttpResponse(
                json.dumps({"job_url": job_url}),
                mimetype="application/json",
                status_code=202,
            )

        # Mode 2: check status / fetch result
        job_url = body.get("job_url") or req.params.get("job_url")
        if job_url:
            job = get_job(cfg, job_url)
            status = job.get("status")

            if status in ("NotStarted", "Running"):
                return func.HttpResponse(
                    json.dumps({"status": status, "job_url": job_url}),
                    mimetype="application/json",
                    status_code=200,
                )

            if status == "Failed":
                return func.HttpResponse(
                    json.dumps({"status": status, "job": job}),
                    mimetype="application/json",
                    status_code=200,
                )

            if status == "Succeeded":
                urls = get_transcript_urls(cfg, job_url)
                result = download_and_normalize(urls, prefer_channel=0, dedupe=True)
                return func.HttpResponse(
                    json.dumps({"status": status, "result": result}),
                    mimetype="application/json",
                    status_code=200,
                )

            return func.HttpResponse(
                json.dumps({"status": status, "job": job}),
                mimetype="application/json",
                status_code=200,
            )

        return func.HttpResponse(
            json.dumps({"error": "Send either 'media_url' to submit, or 'job_url' to check status."}),
            mimetype="application/json",
            status_code=400,
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
