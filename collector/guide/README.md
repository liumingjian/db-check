# Collector guide template

Every release package ships a `GUIDE.md` that tells an engineer how to run the collector on that package's platform. `scripts/build_release_packages.sh` assembles it from the files in this directory. The guide lives next to the collector source, so the `vX.Y.Z` tag that builds a release also fixes the guide that ships with it. This README is not shipped.

## How the guide is assembled

The build takes every `.md` file in `common/`, `db/`, and the directory named after the package's OS (`linux/` or `windows/`). It orders them by file name across all three directories and joins them into one file. The number at the start of each file name sets its place:

| Range | Part | Directory |
| --- | --- | --- |
| `00`–`09` | Title and overview | `common/` |
| `10`–`19` | Unpack the package and check the version | `linux/`, `windows/` |
| `20`–`29` | Run the collector, one file per database | `db/` |
| `30`–`39` | Collect host metrics | `linux/`, `windows/` |
| `40`–`49` | Compress the run directory into a ZIP | `linux/`, `windows/` |
| `50`–`69` | Upload, generate the report, and fix common problems | `common/` |

The build then replaces these placeholders. The example values are for release 1.2.0 on amd64.

| Placeholder | Linux value | Windows value |
| --- | --- | --- |
| `{{VERSION}}` | `1.2.0` | `1.2.0` |
| `{{PACKAGE}}` | `db-collector-1.2.0-linux-amd64` | `db-collector-1.2.0-windows-amd64` |
| `{{BINARY}}` | `db-collector` | `db-collector.exe` |
| `{{COLLECTOR}}` | `./db-collector` | `.\db-collector.exe` |
| `{{SHELL}}` | `bash` | `powershell` |

To read the guide a package will ship, run `make release` and open `dist/<package>/GUIDE.md`.

## Where content goes

Put each sentence in the one place where it is true for every package that gets it.

- **`common/`**: true on every OS and for every database. No commands that differ between shells.
- **`linux/` and `windows/`**: steps that differ by OS, such as unpacking, opening a shell, and compressing the run directory. Each file in `linux/` has a file with the same name in `windows/`, so both guides have the same sections in the same order.
- **`db/`**: one file per database type, the same on every OS. Write commands with `{{COLLECTOR}}` and fence them with `{{SHELL}}`, so one file serves both platforms. Keep each command on one line, because `bash` and PowerShell continue lines differently.

To add a database, add `db/2N-<db-type>.md` and follow the structure of `db/20-mysql.md`: the command, the table of values to replace, and the line about `run_id`. If the database needs an account set up first, add a section before the command, as `db/21-oracle.md` does. To add an OS, add a directory with the same file names as `linux/`, then add the OS to `PLATFORMS` and `guide_values` in the build script.

## Writing rules

- Write in simplified Chinese. The engineers who read the guide work in Chinese, and the collector prints its errors in Chinese.
- Write steps as commands to the reader. One action per numbered step.
- Put the condition before the step: "如果主机上没有 `unzip` 命令，……", not the other way round.
- After a step that prints something, say what the reader should see, such as the version number or the `run_id=` line.
- Write example values that look real, such as `10.0.0.10` and `'ChangeMe'`, not `<host>`. An angle bracket breaks the command if it is pasted unchanged. List every value to replace in a table with the columns 参数, 示例值, and 替换为.
- Wrap every password in single quotes. Single quotes keep special characters literal in both `bash` and PowerShell.
- Name UI elements exactly as the platform shows them, in bold: **选择 ZIP 采集包**. Check the label in `web/src/components/console/` before you write it.
- Write only what the code does. Take flags, defaults, and error text from `collector/internal/cli/`. If a behavior is not in the code or in `docs/`, leave it out.
- Leave out background and design reasons. Never link to `docs/` or other repository files, because the engineer cannot open the repository.

## Change the guide with the collector

Change the guide in the same pull request that changes what an engineer types or sees: a flag, a default, an error message, the run directory layout, or an upload label. The tag then ships the collector and a guide that agree.
