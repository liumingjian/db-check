# Optional diagnostic browser checks

These checks use the real frontend, API, Python parsers, and report renderer. They require a signed-in active user on the generation page. A dedicated temporary API data directory avoids touching existing accounts or customer inputs.

1. Reuse the repository Python environment and generate valid matching inputs:

   ```sh
   uv run --python .venv/bin/python --no-project python tests/browser/diagnostic_fixtures.py /tmp/dbcheck-optional-diagnostics-fixtures
   ```

2. Build `reporter/cmd/db-web`, create a temporary admin with `db-web admin create --username browser --data-dir <temporary-directory>`, and start the API with that directory, the Python environment, and the frontend origin in `--allowed-origins`. Start the frontend with `NEXT_PUBLIC_API_BASE` pointing to that API. In a worktree whose node_modules is symlinked outside the project root, run Next with `--webpack`.

3. Apply the Playwright CLI skill. Open the frontend in a named browser session, sign in, and complete the initial password change. Run:

   ```sh
   playwright-cli -s=diagnostics run-code --filename=tests/browser/optional_diagnostics.js
   ```

   The script verifies ZIP-only entry, invalid ZIPs, type-specific controls, keyboard file-picker access, no-attachment generation, AWR removal/reselection, multiple WDRs, and item associations in a mixed batch. It saves four actual downloaded report ZIPs beside the generated input fixtures. The CLI may retain file-chooser bookkeeping after the script has handled those choosers; close the session after checking the downloads.

4. Verify diagnostic data in the downloaded DOCX files:

   ```sh
   uv run --python .venv/bin/python --no-project python tests/browser/inspect_diagnostic_reports.py /tmp/dbcheck-optional-diagnostics-fixtures
   playwright-cli -s=diagnostics close
   ```

The AWR fixture uses Oracle database ORACC and DBID 2668322570. Both WDR files use the existing valid cluster report, paired to a GaussDB ZIP whose database is postgres. The inspector checks an AWR SQL ID and metrics, two WDR source rows, and absence of cross-item diagnostic content. Placeholder HTML in transport tests does not establish parser success.

## Attachment recovery

Generate fixtures in `/tmp/dbcheck-diagnostic-recovery-fixtures` and start the same real service flow. Run `playwright-cli -s=diagnostics run-code --filename=tests/browser/diagnostic_recovery.js`, then `uv run --python .venv/bin/python --no-project python tests/browser/inspect_diagnostic_reports.py /tmp/dbcheck-diagnostic-recovery-fixtures --recovery`.

The recovery suite rejects malformed AWR/WDR, swapped diagnostic types, AWR database name and DBID mismatches, and a WDR database name mismatch. Each case retains both selected ZIPs and the other item's valid attachment, removes only the bad attachment, selects a matching replacement, and downloads real reports. The inspector verifies both documents contain their own diagnostic data.

Run `diagnostic_recovery_edges.js` in the same signed-in session to verify removing a bad optional attachment permits generation without it, and a delayed response from the real validation service is discarded after replacement. It saves `removed-optional.zip` and `stale-response-corrected.zip`.

## Correspondence guidance

Generate fixtures in `/tmp/dbcheck-diagnostic-guidance-fixtures` and use the same real service flow. Run `playwright-cli -s=diagnostics run-code --filename=tests/browser/diagnostic_guidance.js`, then `uv run --python .venv/bin/python --no-project python tests/browser/inspect_diagnostic_reports.py /tmp/dbcheck-diagnostic-guidance-fixtures --guidance`.

The suite checks immediate manual-check guidance, the optional “校验附件” action, AWR database-name/DBID confirmation, WDR database-name confirmation, and the remaining uncertainty about identity or time. It verifies that parser failures and known mismatches remain errors, replacement discards a delayed real confirmation, and removing an item clears its guidance. The suite holds the actual generation POST and verifies ZIP and attachment edits stay unavailable until submission finishes, while edits during temporary validation still discard stale responses. A corrected mixed batch downloads successfully despite uncertain time correspondence; the inspector checks each document's diagnostic content.
