import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://api.box.com/oauth2/token"

CLIENT_ID = os.getenv("BOX_CLIENT_ID")
CLIENT_SECRET = os.getenv("BOX_CLIENT_SECRET")


def get_access_token() -> str:
    """
    Returns a valid access token.

    Priority:
    1) BOX_TOKEN (developer token shortcut) if set
    2) BOX_ACCESS_TOKEN if still valid
    3) Refresh via BOX_REFRESH_TOKEN (+ client id/secret), persist rotated tokens
    """
    # 1) Developer token shortcut (no refresh possible)
    dev = os.getenv("BOX_TOKEN") or os.getenv("BOX_DEVELOPER_TOKEN")
    if dev:
        return dev

    access_token = os.getenv("BOX_ACCESS_TOKEN")
    refresh_token = os.getenv("BOX_REFRESH_TOKEN")

    # 2) If we have an access token, test it
    if access_token:
        r = requests.get(
            "https://api.box.com/2.0/users/me",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if r.status_code == 200:
            return access_token

    # 3) Refresh flow
    if not refresh_token:
        raise RuntimeError(
            "Missing BOX_REFRESH_TOKEN (and no BOX_TOKEN). "
            "Either set BOX_TOKEN to a Box Developer Token, or run your Flask OAuth flow once."
        )

    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Missing BOX_CLIENT_ID/BOX_CLIENT_SECRET for refresh flow.")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    resp = requests.post(TOKEN_URL, data=data, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Token refresh failed: {resp.status_code} {resp.text}")

    tok = resp.json()
    new_access = tok["access_token"]
    new_refresh = tok["refresh_token"]  # Box rotates refresh tokens

    _update_env("BOX_ACCESS_TOKEN", new_access)
    _update_env("BOX_REFRESH_TOKEN", new_refresh)
    return new_access


def _update_env(key: str, value: str) -> None:
    path = ".env"
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        lines = []

    out = []
    found = False
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"{key}={value}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
