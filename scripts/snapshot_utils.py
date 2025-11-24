# === snapshot_utils.py ===

import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Root for snapshots – default to your qma_live Dropbox app folder
SNAPSHOT_DIR = os.path.join(
    os.getenv("DROPBOX_ROOT", "C:/Users/colby/Dropbox/Apps/qma_live"),
    "spy",
    "snapshots",
)
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# Toggle for verbose debugging
DEBUG_SNAPSHOT = True


def _dbg(*args):
    """Centralized snapshot debug printer."""
    if DEBUG_SNAPSHOT:
        print("[snapshot]", *args)


def _check_kaleido_available() -> bool:
    """Return True if Plotly Kaleido is available, else False (no exception)."""
    try:
        import plotly.io as pio  # type: ignore

        kaleido_mod = getattr(pio, "kaleido", None)
        scope = getattr(kaleido_mod, "scope", None) if kaleido_mod is not None else None
        return scope is not None
    except Exception:
        return False


def write_snapshot_bundle(charts: Dict[str, Any], notes: str = "") -> str:
    """
    Save a timestamped snapshot ZIP with:
    - PNG charts (from Plotly figures)
    - Summary notes (TXT)

    Output goes to snapshots folder for GPT review.

    This version includes extensive debugging:
    - Logs chart keys and types
    - Checks Kaleido availability
    - Verifies PNG file creation and size
    - Prints final ZIP contents
    """
    _dbg("=== write_snapshot_bundle START ===")

    # --- Basic context info ---
    _dbg("SNAPSHOT_DIR:", SNAPSHOT_DIR)
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)

    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%a_%H-%M").lower()
    base_name = f"spy_snapshot_{timestamp}"
    bundle_path = os.path.join(SNAPSHOT_DIR, f"{base_name}.zip")

    _dbg("timestamp:", timestamp)
    _dbg("base_name:", base_name)
    _dbg("bundle_path:", bundle_path)

    # --- Charts info ---
    chart_keys = list(charts.keys())
    _dbg("chart keys:", chart_keys)
    _dbg("chart count:", len(chart_keys))

    for k, fig in charts.items():
        _dbg(f"chart '{k}' type:", type(fig))

    # --- Kaleido check (for write_image) ---
    kaleido_ok = _check_kaleido_available()
    _dbg("kaleido available:", kaleido_ok)
    if not kaleido_ok:
        _dbg(
            "WARNING: Kaleido is NOT available. "
            "Plotly write_image will fail. Install with: pip install -U kaleido"
        )

    # --- Create ZIP and write contents ---
    added_files = []

    with zipfile.ZipFile(bundle_path, "w") as zf:
        # ----- PNG charts -----
        for title, fig in charts.items():
            fname = f"{title.replace(' ', '_').lower()}_{timestamp}.png"
            png_path = os.path.join(SNAPSHOT_DIR, fname)
            _dbg(f"processing chart: '{title}' → {fname}")
            _dbg("  png_path:", png_path)

            # Validate object has a write_image method
            if not hasattr(fig, "write_image"):
                _dbg(
                    f"  ERROR: object for '{title}' has no write_image(); "
                    f"type={type(fig)} – skipping."
                )
                continue

            try:
                _dbg(f"  calling write_image for '{title}'")
                fig.write_image(png_path, format="png", width=1200, height=600, scale=2)

                exists = os.path.exists(png_path)
                size = os.path.getsize(png_path) if exists else "n/a"
                _dbg(f"  wrote PNG? exists={exists}, size={size}")

                if not exists or (isinstance(size, int) and size <= 0):
                    raise RuntimeError(
                        f"write_image produced no file or zero-size file for '{title}'"
                    )

                # Add to ZIP
                zf.write(png_path, arcname=fname)
                added_files.append(fname)
                _dbg(f"  added to zip as: {fname}")

            except Exception as e:
                _dbg(f"  ERROR writing chart '{title}': {e}")

            finally:
                # Clean up PNG if it exists
                try:
                    if os.path.exists(png_path):
                        os.remove(png_path)
                        _dbg(f"  removed temp PNG: {png_path}")
                except Exception as e:
                    _dbg(f"  WARNING: could not remove temp PNG {png_path}: {e}")

        # ----- Meta notes file -----
        meta_name = f"meta_{timestamp}.txt"
        meta_path = os.path.join(SNAPSHOT_DIR, meta_name)
        meta_text = notes or f"Snapshot generated at {now.isoformat()}\n"

        try:
            _dbg("writing meta file:", meta_path)
            with open(meta_path, "w") as f:
                f.write(meta_text)

            zf.write(meta_path, arcname=meta_name)
            added_files.append(meta_name)
            _dbg("added meta to zip:", meta_name)
        except Exception as e:
            _dbg("ERROR writing meta file:", e)
        finally:
            try:
                if os.path.exists(meta_path):
                    os.remove(meta_path)
                    _dbg("removed temp meta file:", meta_path)
            except Exception as e:
                _dbg("WARNING: could not remove temp meta file:", e)

    # --- Inspect ZIP contents after creation ---
    _dbg("zip created at:", bundle_path)

    try:
        with zipfile.ZipFile(bundle_path, "r") as zf:
            contents = zf.namelist()
            _dbg("zip contents:", contents)
            _dbg("zip file count:", len(contents))
    except Exception as e:
        _dbg("ERROR reopening zip to inspect contents:", e)
        contents = []

    # --- Summary of what happened ---
    _dbg("files attempted to add:", added_files)
    if not contents:
        _dbg("WARNING: ZIP is empty – likely all chart writes failed.")
    else:
        _dbg("ZIP contains", len(contents), "file(s).")

    _dbg("=== write_snapshot_bundle COMPLETE ===")
    return bundle_path


def save_snapshot_for_gpt(ticker: str, spot, target, df, charts: Dict[str, Any]) -> str:
    """
    High-level snapshot save logic: takes current data and visuals and creates export bundle.

    ticker : e.g. "SPY"
    spot   : current underlying spot (float)
    target : gap target / reference level
    df     : raw DataFrame used for context (currently just logged)
    charts : dict of Plotly figures keyed by descriptive name
    """
    _dbg("=== save_snapshot_for_gpt START ===")
    _dbg("ticker:", ticker)
    _dbg("spot:", spot)
    _dbg("target:", target)

    try:
        shape = getattr(df, "shape", None)
        _dbg("df shape:", shape)
    except Exception:
        _dbg("df shape: <unavailable>")

    # Compose summary text
    now = datetime.now()
    summary = (
        f"Spot: {spot}\n"
        f"Target: {target}\n"
        f"Date: {now:%Y-%m-%d}\n"
        f"Time: {now:%H:%M}\n"
    )
    _dbg("summary text:")
    _dbg(summary)

    bundle_path = write_snapshot_bundle(charts=charts, notes=summary)
    _dbg("=== save_snapshot_for_gpt COMPLETE ===")
    _dbg("bundle_path:", bundle_path)
    return bundle_path
