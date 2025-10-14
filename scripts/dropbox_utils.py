# === dropbox_utils.py ===
import dropbox
from io import BytesIO
import pandas as pd
from config import DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN



def get_dropbox_client():
    """Connect to Dropbox using a permanent refresh token."""
    return dropbox.Dropbox(
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET,
    )


# --- Upload ---
def upload_file(local_path: str, dropbox_path: str):
    dbx = get_dropbox_client()
    with open(local_path, "rb") as f:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
    print(f"[Dropbox] Uploaded: {local_path} → {dropbox_path}")


# --- Download ---
def download_file(dropbox_path: str, local_path: str):
    dbx = get_dropbox_client()
    metadata, res = dbx.files_download(dropbox_path)
    with open(local_path, "wb") as f:
        f.write(res.content)
    print(f"[Dropbox] Downloaded: {dropbox_path} → {local_path}")


# --- Read Excel directly ---
def read_excel(dropbox_path: str, **kwargs) -> pd.DataFrame:
    dbx = get_dropbox_client()
    _, res = dbx.files_download(dropbox_path)
    return pd.read_excel(BytesIO(res.content), **kwargs)
