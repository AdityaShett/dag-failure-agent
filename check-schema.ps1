$ErrorActionPreference = "Stop"

$PROJECT = "dag-failure-agent-505623"
$DATASET = "dag_failure_agent"

Write-Host "=== Checking s_llm / s_retrieval column mode in confidence_signals ===" -ForegroundColor Cyan

bq show --schema --format=prettyjson "${PROJECT}:${DATASET}.confidence_signals" | Select-String "s_llm|s_retrieval" -Context 0,2

Write-Host "`nIf mode is 'NULLABLE' for both: safe to just stop writing them (Edit 4 above)." -ForegroundColor Green
Write-Host "If mode is 'REQUIRED' for either: run this first to relax it before deploying:" -ForegroundColor Yellow
Write-Host ""
Write-Host '  bq update --schema_update_option=ALLOW_FIELD_RELAXATION `'
Write-Host '    --schema s_llm:FLOAT,s_retrieval:FLOAT `'
Write-Host "    ${PROJECT}:${DATASET}.confidence_signals"
Write-Host ""
Write-Host '(bq update with a partial schema like this only relaxes the named fields' -ForegroundColor Yellow
Write-Host 'to NULLABLE; it does not remove other columns or data.)' -ForegroundColor Yellow
