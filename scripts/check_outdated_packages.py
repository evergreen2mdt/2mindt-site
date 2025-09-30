import subprocess
import json
from tqdm import tqdm
import time
import os
from datetime import datetime

LOG_PATH = r"C:\2mdt\2mindt-site\logs\outdated_packages.txt"

def get_outdated_packages():
    print("Checking for outdated packages...")

    # Simulate initial progress (checking environment)
    for _ in tqdm(range(10), desc="Initializing pip check", unit="step"):
        time.sleep(0.05)

    result = subprocess.run(
        ["python", "-m", "pip", "list", "--outdated", "--format=json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    # Clean any pip [notice] lines
    raw_lines = result.stdout.splitlines()
    json_lines = [line for line in raw_lines if not line.startswith("[notice]")]
    json_text = "\n".join(json_lines)

    try:
        packages_info = json.loads(json_text)
        packages = [pkg["name"] for pkg in packages_info]
    except json.JSONDecodeError:
        print("[!] Failed to parse pip output.")
        packages_info = []
        packages = []
        json_text = "[!] Could not parse pip output:\n\n" + result.stdout

    return packages, packages_info, json_text

def write_log_human_readable(packages_info):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"Last run: {timestamp}\n\n")

        if not packages_info:
            f.write("All packages are up to date.\n")
            return

        f.write("Outdated Python Packages:\n")
        f.write("==========================\n\n")
        for pkg in packages_info:
            f.write(f"{pkg['name']:30} Current: {pkg['version']:10}  Latest: {pkg['latest_version']}\n")

def show_progress(packages, packages_info):
    if not packages:
        print("All packages are already up to date.")
        return

    print(f"\nFound {len(packages)} outdated packages:\n")

    for i in tqdm(range(len(packages)), desc="Evaluating packages", unit="pkg"):
        pkg = packages[i]
        info = packages_info[i]
        print(f"Package: {pkg} | Current: {info['version']} -> Latest: {info['latest_version']}")
        time.sleep(0.1)  # Visual delay

if __name__ == "__main__":
    print("=========================================")
    print("Python Outdated Package Inspector")
    print("=========================================\n")

    packages, packages_info, raw_output = get_outdated_packages()
    write_log_human_readable(packages_info)
    show_progress(packages, packages_info)

    print("\nOutdated package list saved to:")
    print(LOG_PATH)
