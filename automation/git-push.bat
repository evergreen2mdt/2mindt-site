@echo off
cd /d C:\2mdt\2mindt-site

:: Get timestamp (YYYY-MM-DD_HH:MM:SS format)
for /f "tokens=1-3 delims=/: " %%a in ("%date%") do (
    set datestr=%%c-%%a-%%b
)
for /f "tokens=1-2 delims=: " %%a in ("%time%") do (
    set timestr=%%a-%%b
)
set commitmsg=Update: push latest site structure and files (%datestr%_%timestr%)

echo === Staging all changes (adds, edits, deletions) ===
git add -A

echo === Committing changes ===
git commit -m "%commitmsg%" || echo Nothing to commit

echo === Pushing to origin/main ===
git push origin main

echo.
echo === Git push complete ===
pause
