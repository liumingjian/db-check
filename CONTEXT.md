# Database inspection platform

This context covers distributing the collector to engineers and generating database reports from the diagnostic inputs they bring back.

## Language

### Collectors

**Collector**:
The single `db-collector` tool engineers run on a customer database host to produce a diagnostic ZIP. One collector covers every supported database type.
_Avoid_: Tool, per-database collector, 采集器 when a specific version is meant

**Collector release**:
One published, versioned build of the collector, traceable to a git tag and commit. Only CI creates releases.
_Avoid_: Collector, tool version, package

**Release package**:
The platform-specific artifact (OS and architecture) inside a collector release, identified by its checksum.
_Avoid_: Release, installer, tool

**Release status**:
The standing of a collector release: **pre-release** (visible to admins only), **latest** (the one recommended release; the previous latest becomes deprecated), **deprecated** (downloadable with a warning), or **revoked** (blocked for engineers, always with a revocation reason).
_Avoid_: Enabled, published, archived, beta

**Download record**:
An audit entry stating which user downloaded which release package and when.
_Avoid_: Download log, access log

### Reports

**Report task (报告)**:
A submitted batch of report items with a collective outcome and, when report generation succeeds, a downloadable collection of reports. A task belongs to the user who submitted it. A task can contain both successful and failed items.
_Avoid_: Report item when referring to the whole submission

**Report item**:
One report-generation unit consisting of a primary diagnostic ZIP and its paired optional AWR or WDR inputs. Each item has its own outcome and can produce one report document.
_Avoid_: Report task when referring to one unit within a submission

**Submitter (提交人)**:
The user who submitted a report task. Engineers see only tasks they submitted; admins see all tasks.
_Avoid_: Owner, creator

### Users

**User (用户)**:
A person with an account on the platform, holding exactly one role and one account status.
_Avoid_: Account when the person is meant, member (成员)

**Engineer (普通用户)**:
The role for users who download collectors and generate reports.
_Avoid_: Normal user, regular user, customer

**Admin (管理员)**:
The role that has every engineer capability and also approves users and changes release status.
_Avoid_: Superuser, operator

**Account status**:
The lifecycle state of a user: **pending** (registered, awaiting admin approval), **active**, **rejected** (may register again), or **disabled** (cannot sign in; submitted tasks are kept).
_Avoid_: Locked, banned, inactive
