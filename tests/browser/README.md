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
