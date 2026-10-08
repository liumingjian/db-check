---
name: deploy-db-check
description: Deploy or update this db-check platform on its intranet Linux host, verify production services, or roll back a failed deployment.
---

# Deploy db-check

Deploy the API and console together from one committed revision. Read
[references/production.md](references/production.md) before deployment or rollback.
Use [docs/deployment.md](../../../docs/deployment.md) in the repository root for
application configuration and first-admin semantics.

1. Inspect the current revision and tracked changes. Complete the repository's
   required checks for the intended revision. Commit intended source changes
   before packaging; leave unrelated untracked files outside the package.
2. Run `bash .agents/skills/deploy-db-check/scripts/package.sh <new-output-dir>`
   from the repository. The helper archives tracked HEAD, builds the Linux amd64
   API from that archive, and writes revision, toolchain, and SHA256 metadata.
3. Verify the target's current PM2 definitions, runtime versions, ports, data
   path, and free space. Prepare a complete new release before stopping services.
4. Follow the runbook's backup, cutover, and verification sequence. For updates,
   preserve the complete persistent data directory and existing accounts. Restore
   the prior release when verification fails before new writes. Follow the
   runbook's recovery boundary after migrations or public user writes.
5. Report the deployed revision, application URL, service state, verification
   results, and backup location. Leave credentials in the remote private file
   or the session's credential channel.

This skill manages only `dbcheck-api` and `dbcheck-web`. Collector publishing is
a separate workflow. Use credentials supplied by the user for the current task;
keep passwords and tokens out of skill files, packages, commands printed to the
user, and version control. Existing deployment authorization covers the requested
deployment; a different target or unrelated service requires separate scope.
