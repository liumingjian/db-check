# Oracle reference comparison

The reviewed sources are [EasyDBA OracleRules.cs](https://gitee.com/dyx88168/EasyDBA/blob/a014d76f3fe8886669feb8d64049a2a7a54f135e/src/EasyDBA.Core/Rules/Catalogs/OracleRules.cs), its [RuleEvaluator.cs](https://gitee.com/dyx88168/EasyDBA/blob/a014d76f3fe8886669feb8d64049a2a7a54f135e/src/EasyDBA.Core/Engine/RuleEvaluator.cs), and [DBCheck oracle.yaml](https://github.com/fiyo/DBCheck/blob/8c0de837141723abed7d094fb41839e611871b1e/modules/pro/rules/builtin/oracle.yaml). Both source trees were available for comparison. Thresholds belong to Analyzer rules; Collector payloads retain facts for replay.

| Check | Verified comparison and selected behavior |
| --- | --- |
| TEMP and SYSAUX | Spec requires existing capacity thresholds >80% warning, >90% critical. EasyDBA TEMP uses 85/95; DBCheck TEMP has 90 high and SYSAUX 85 medium. Project capacity thresholds are more conservative. |
| Weak password profiles | EasyDBA nonempty row count is critical. Check 5.6 uses >=1 critical. |
| Fixed-user database links | EasyDBA uses >=1 warning, >=10 critical. Check 5.10 uses those inclusive thresholds. Metadata indicates possible stored credentials, never proves a password exists. |
| Unlimited resource profiles | EasyDBA uses >=1 warning, >=5 critical. Check 5.12 uses those inclusive thresholds. |
| Audit disabled, flashback NO, maximum performance protection | EasyDBA contains evaluator returns critical although descriptions sometimes say warning. Checks 5.8, 6.6 and 9.4 select critical conservatively and document that discrepancy. Approved customer policy still needs DBA review. |
| Data Guard lag | EasyDBA description says transport >30 seconds warrants warning and apply >60 seconds is serious, but its display evaluator never emits an abnormal result. Checks 9.1/9.2 use >30 warning and >60 critical for both metrics as a conservative project baseline, without copying the nonfunctional evaluator. |
| Backup age | Neither source supplies the same successful-data-backup age metric. >48 hours warning, >168 hours or no successful data backup critical is the project baseline. Missing source access is a coverage gap, not evidence that backups are absent. |
| ASM capacity | No equivalent executable reference threshold was found. >80/>90 capacity thresholds are the project baseline. |
| Wait latency, latch ratios, resource use, time model, UNDO errors | No same-unit reference thresholds were found. Values in the performance document are explicit project baselines. |
| Segment concentration, recycle bin, patch/registry states | No comparable numeric threshold was found. Capacity reuse and positive recycle-bin advisory are project policies; patch and registry findings use observed operations/states. |

EasyDBA's TDE rule with maxvalue >=0 would always report critical, and its Data Guard display rule cannot produce a verdict. These reference defects are not copied. Uploaded AWR remains separate from Collector data under ADR 0004.
