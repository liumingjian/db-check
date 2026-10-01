---
status: accepted
---

# Publish collector releases only from CI, through the platform

Engineers used to receive the collector by offline hand-off, so nobody could tell which build was in the field. Every collector release is now created by CI from a `vX.Y.Z` git tag. A self-hosted GitHub Actions runner inside the intranet builds the release packages and pushes them, with their metadata, to the platform's publish API. The platform stores the packages and serves every download itself. Admins change release status but never upload files, so each package traces back to a tag and commit.

## Considered Options

- **Admin upload in the web UI**: rejected. It breaks tag-to-package traceability, and a hand-built binary could reach customer hosts.
- **Downloads served from GitHub Releases, platform keeps metadata only**: rejected. Downloads would bypass platform authorization and download records.
- **Platform polls GitHub Releases on a schedule**: viable, because the intranet reaches github.com. Rejected in favour of the runner push: a failed publish surfaces in the CI run, and the platform needs no scheduler.
- **Object storage (S3/MinIO) holding the packages**: rejected for now. It adds a deployment dependency, and the platform still has to proxy downloads to authorize and record them.

## Consequences

- The platform exposes a publish API authenticated by a CI machine credential, separate from user sign-in.
- CI fails the release when the tag disagrees with the collector's built-in version.
- Moving the platform out of reach of the intranet runner means revisiting this ADR.
