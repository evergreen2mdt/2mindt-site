@echo off
cd /d C:\2mdt\2mindt-site

echo === Staging all changes ===
git add .

echo === Committing changes ===
git commit -m "Update: push latest site structure and files"

echo === Pushing to origin/main ===
git push origin main

echo.
echo === Git push complete ===
pause
