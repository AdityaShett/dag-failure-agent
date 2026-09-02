$ErrorActionPreference = "Stop"

Write-Host "=== Cleanup: removing build artifacts and superseded files ===" -ForegroundColor Cyan

# --- Build artifacts (regenerated automatically, safe anytime) ---
Get-ChildItem -Path . -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
if (Test-Path ".\.pytest_cache") { Remove-Item ".\.pytest_cache" -Recurse -Force }

# --- Tree dumps used only in chat, not part of the project ---
Remove-Item ".\tree.txt" -ErrorAction SilentlyContinue
Remove-Item ".\tree2.txt" -ErrorAction SilentlyContinue

# --- Superseded weight-tuning scripts (5-signal and v2 grid search --
#     replaced by the simple 3-signal manual scorer below) ---
Remove-Item ".\tune_weights.py" -ErrorAction SilentlyContinue
Remove-Item ".\tune_weights_3signal.py" -ErrorAction SilentlyContinue
Remove-Item ".\tune_weights_v2.py" -ErrorAction SilentlyContinue

# --- One-time migration/diagnostic scripts -- job already confirmed done
#     (fallback contamination verified at 0% in your last run) ---
Remove-Item ".\00-apply-fix.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\01-check-dag6.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\02-clear-data.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\03-redeploy.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\04-run-batch.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\05-verify-fix.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\06-check-balance.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\06b-sample-rejects.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\07-tune-weights.ps1" -ErrorAction SilentlyContinue
Remove-Item ".\fallback_check.py" -ErrorAction SilentlyContinue

# --- Old ad hoc test/experiment scripts, superseded utilities, and backups ---
Remove-Item ".\local_diff_test.py" -ErrorAction SilentlyContinue
Remove-Item ".\test_objective2.py" -ErrorAction SilentlyContinue
Remove-Item ".\publish_test_message.py" -ErrorAction SilentlyContinue
Remove-Item ".\label_decisions_pre_fix.json" -ErrorAction SilentlyContinue

Write-Host "`nDone. Run 'git status' to review before committing." -ForegroundColor Green
git status

Write-Host "`n--- Files NOT deleted -- confirm you don't need these before removing ---" -ForegroundColor Yellow
Write-Host "  publish_batch.py    -- superseded by publish_batch_diverse.py? or still used for quick single-scenario tests"
Write-Host "  seed_history.py     -- one-time GCS/BigQuery seeding script; keep if you might need to reseed test data"
Write-Host "  query.sql           -- unclear current purpose; check what it's used for before removing"
Write-Host "  setup.py            -- check if anything still does 'pip install -e .' against this before removing"

Write-Host "`nOnce you're happy with 'git status' above, commit with:" -ForegroundColor Cyan
Write-Host '  git add -A'
Write-Host '  git commit -m "Clean up superseded tuning scripts, one-time migration tooling, and build artifacts"'
Write-Host '  git push'
