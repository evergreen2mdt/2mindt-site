# === dropbox_utils.py ===
import dropbox
import os
from io import BytesIO
import pandas as pd

# Initialize Dropbox client
def get_dropbox_client():
    """Connect to Dropbox using token stored in Streamlit Secrets or env var."""
    import streamlit as st
    token = None

    # Try secrets first (Streamlit Cloud)
    if "DROPBOX_TOKEN" in st.secrets:
        token = st.secrets["DROPBOX_TOKEN"]
    # Fallback to environment variable (local testing)
    elif os.getenv("DROPBOX_TOKEN"):
        token = os.getenv("DROPBOX_TOKEN")

    if not token:
        raise ValueError("Dropbox token not found. Set DROPBOX_TOKEN in Streamlit Secrets or env vars.")

    return dropbox.Dropbox(token)

# --- Upload ---
def upload_file(local_path: str, dropbox_path: str):
    """Upload local file to Dropbox path."""
    dbx = get_dropbox_client()
    with open(local_path, "rb") as f:
        dbx.files_upload(
            f.read(),
            dropbox_path,
            mode=dropbox.files.WriteMode("overwrite")
        )
    print(f"[Dropbox] Uploaded: {local_path} → {dropbox_path}")

# --- Download ---
def download_file(dropbox_path: str, local_path: str):
    """Download file from Dropbox to local path."""
    dbx = get_dropbox_client()
    metadata, res = dbx.files_download(dropbox_path)
    with open(local_path, "wb") as f:
        f.write(res.content)
    print(f"[Dropbox] Downloaded: {dropbox_path} → {local_path}")

# --- Read Excel directly ---
def read_excel(dropbox_path: str, **kwargs) -> pd.DataFrame:
    """Read Excel directly from Dropbox into DataFrame."""
    dbx = get_dropbox_client()
    metadata, res = dbx.files_download(dropbox_path)
    return pd.read_excel(BytesIO(res.content), **kwargs)
