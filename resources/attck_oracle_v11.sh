#!/bin/bash
# 获取所有Oracle实例名
ps -ef | grep pmon | grep -v grep| grep -v ASM | awk -F_ '{print $3}' | while read -r instance
do
  echo "Instance Name: $instance"
os=`uname -a|awk '{print $1}'`
if [ $os = 'HP-UX' ];then
NLS_LANG=AMERICAN_AMERICA.UTF8;export NLS_LANG 
ORACLE_SID=$instance ;export ORACLE_SID 
elif [ $os = 'AIX' ];then
NLS_LANG=AMERICAN_AMERICA.UTF8;export NLS_LANG 
ORACLE_SID=$instance ;export ORACLE_SID 
LANG=zh_CN.UTF-8;export LANG
elif [ $os = 'SunOS' ];then
NLS_LANG=AMERICAN_AMERICA.UTF8;export NLS_LANG 
ORACLE_SID=$instance ;export ORACLE_SID 
LANG=zh_CN.UTF-8;export LANG
else
NLS_LANG=AMERICAN_AMERICA.UTF8;export NLS_LANG 
LANG=zh_CN.UTF-8;export LANG
ORACLE_SID=$instance ;export ORACLE_SID 
fi
##### This script is used to check the health of the database server, and automatically generate health inspection report Verson 1.0#####

db_file_name=db_check_`hostname`_$instance.xml
os_file_name=os_check_`hostname`_$instance.xml
health=health_check_`hostname`_${instance}_`date +"%Y%m%d%H%M"`
RMFILE=rm

######## SQL #############
sqlplus -s "/ as sysdba" <<EOF
set echo off
set termout off
set pagesize
set linesize 999
set long 9999
set feedback off

---set markup html on entmap ON spool on preformat off---
spool database_check_info.txt

prompt <?xml version="1.0" encoding="UTF-8"?>

-----Database Name  数据库名称-------------
prompt <datas>
prompt <dbName>
select name from v\$database;
prompt </dbName>

-----Instance Name  数据库实例名-------------
prompt <instName1>
select instance_name from v\$instance;
prompt </instName1>

-----spfile  是否使用spfile-------------
prompt <spfile1>
select VALUE from gv\$parameter where name ='spfile' and  inst_id=1;
prompt </spfile1>

-----DBID  数据库DBID-------------
prompt <DBID1>
select DBID from v\$database;
prompt </DBID1>


-----RAC or single instance  是否为RAC-------------
prompt <isRac>
select decode(VALUE,'TRUE','Yes','No') from v\$option where PARAMETER='Real Application Clusters';
prompt </isRac>

-----RDBMS Version  数据库版本-------------
prompt <VersionNo>
select VERSION from product_component_version where rownum<2;
prompt </VersionNo>

-----Character set  数据库字符集-------------
prompt <Characterset>
select VALUE from NLS_DATABASE_PARAMETERS where PARAMETER='NLS_CHARACTERSET';
prompt </Characterset>

-----Total Datafile Size 数据文件所占的磁盘空间-------------
prompt <DatafileTotal>
col sum1 for 999999
select ceil(sum(bytes)/1024/1024/1024) sum1 from v\$datafile;
prompt </DatafileTotal>

-----SGA Size  SGA大小---------------
prompt <SgaSize1>
select VALUE/1024/1024||' MB' SGA_TARGET from v\$parameter where name='sga_target';
prompt </SgaSize1>

-----Database Block Size 数据库块大小---------------
prompt <DBBLOCKSIZE>
select VALUE/1024||' KB' DB_BLOCK_SIZE from v\$parameter where name='db_block_size';
prompt </DBBLOCKSIZE>

-----Tablespace count 表空间个数---------------
prompt <Tbscount>
col num for 9999 
select count(1) num from dba_tablespaces; 
prompt </Tbscount>

-----Datafile count 数据文件个数---------------
prompt <dbfilecount>
col dbfs for 9999
select count(1) dbfs from dba_data_files;
prompt </dbfilecount>

-----Conctolfile count 控制文件个数---------------
prompt <contfilecount>
col ctls for 9999
select count(1) ctls from v\$controlfile;
prompt </contfilecount>

-----Redo log information  redo日志大小---------------
prompt <redosize>
col M for 9999
select bytes/1024/1024 M from v\$log where rownum < 2;
prompt </redosize>

-----redo count redo文件个数---------------
prompt <redonumber>
col regs for 9999 
select count(group#) regs from gv\$log;
prompt </redonumber>

-----redo count redo文件成员个数---------------
prompt <redomem>
col rens for 9999
select count(*) rens from (select distinct bytes from v\$log);
prompt </redomem>

-----在线日志切换频率---------------
prompt <RedoSwitch1>
col swins for 9999
alter session set nls_date_format='yyyy-mm-dd'; 
select TRUNC(avg(switch_time)) swins from 
(select a.thread#,(a.first_time-b.first_time)*24 switch_time from gv\$log_history a,gv\$log_history b
where a.thread#=b.thread# and a.sequence#=b.sequence#+1 and a.first_time > to_date(sysdate-7,'yyyy-mm-dd') ) where thread#=1;
prompt </RedoSwitch1>


-----archive log mode  是否归档---------------
prompt <logmode>
select decode(LOG_MODE,'ARCHIVELOG','ARCHIVELOG','NOARCHIVE') from v\$database;
prompt </logmode>

prompt <activesess>
select inst_id,count(1) from gv\$session where status='ACTIVE' group by inst_id;
prompt </activesess>

-----Alert Log information  trace路径---------------
prompt <AlertLog>
select value from v\$parameter where name='background_dump_dest';
prompt </AlertLog>

-----spfile parameters  spfile参数内容---
prompt <Parameters1>
set pages 300
COL name for a34
COL value for a70
select name, value from v\$spparameter where value is not null;
prompt </Parameters1>


-----controlfile name path 控制文件路径----------
prompt <CtlfileName>
set lines 300
set pages 300
col NAME FOR A70
select NAME from v\$controlfile;
prompt </CtlfileName>

-----redo  路径----------
prompt <RedoName>
set pages 300
col MEMBER for a70
select GROUP#,TYPE,MEMBER from v\$logfile;
prompt </RedoName>

prompt <RedoInfo>
set line 300
select GROUP#,THREAD# ,SEQUENCE#,BYTES,MEMBERS,ARCHIVED,STATUS,FIRST_TIME from v\$log;
prompt </RedoInfo>

prompt <Archcheck>
archive log list;
prompt </Archcheck>

-----数据文件名称和大小统计----------
prompt <DatafileName>
set line 300
col file_name for a50
col tablespace_name for a10
col status for a10
col AUTOEXT for a10
col MAX_GB for 99999999
select /*+ rule */ FILE_NAME,tablespace_name,status,round(bytes/1024/1024/1024,2) CUR_GB,AUTOEXTENSIBLE AUTOEXT,round(MAXBYTES/1024/1024/1024,2) MAX_GB
from dba_data_files   ORDER BY 1;
prompt </DatafileName>


-----sequence 使用率----------
PROMPT <dbSequence>
column order_flag  format a4
column exec_time      format date  
column sequence_owner  format a30
column sequence_name   format a30                                                                                                  
column CNT_LESS_200       format 999999999
set head on
SELECT sequence_owner,count(1) CNT_LESS_200 from dba_sequences
where (order_flag='Y'  or cache_size <200)
and sequence_owner not in('SYSTEM','APPQOSSYS','FLOWS_FILES','OWBSYS','SYSMAN','SYS','CTXSYS','MDSYS','OLAPSYS','WMSYS','EXFSYS','LBACSYS','WKSYS','XDB','ORDDATA','SCOTT','APEX_030200','DBSNMP','ORDSYS','OUTLN')
and sequence_owner not like 'FLOWS%'
and sequence_owner not like 'WK%'
group by sequence_owner order by  2 desc
/
PROMPT </dbSequence>

-----sequence2 使用率----------
PROMPT <dbSequence2>
column order_flag  format a4
column exec_time      format date  
column sequence_owner  format a30
column sequence_name   format a30                                                                                                  
column CNT_LESS_200       format 999999999
set head on
select sequence_owner,sequence_name from dba_sequences
where (last_number/max_value)>0.8
and CYCLE_FLAG='N' 
and sequence_owner not in('SYSTEM','APPQOSSYS','FLOWS_FILES','OWBSYS','SYSMAN','SYS','CTXSYS','MDSYS','OLAPSYS','WMSYS','EXFSYS','LBACSYS','WKSYS','XDB','ORDDATA','SCOTT','APEX_030200','DBSNMP','ORDSYS','OUTLN')
and sequence_owner not like 'FLOWS%'
and sequence_owner not like 'WK%'
/
PROMPT </dbSequence2>

---------------
prompt <recover_datafile>
SELECT FILE#               ,
ONLINE_STATUS       ,
ERROR               ,
CHANGE#             ,
TIME                
FROM v\$recover_file ;
prompt </recover_datafile>

-------表空间使用率--------
prompt <TbsFree>
SET LIN 200
SET PAGES 10
SET ECHO OFF
set heading on
SET TERMOUT OFF
SET TRIMOUT ON
SET trimspool on
SET FEEDBACK off
col tablespace_name for a30
col status for a10
col DB_UNIQUE_NAME for a15
col tablespace_name for a30
col status for a10
SET tab off
COLUMN FNM NEW_VALUE V1;
SELECT SYS_CONTEXT ('USERENV', 'DB_UNIQUE_NAME') DB_UNIQUE_NAME, 
      D.TABLESPACE_NAME,
      MAX_SPACE "MAX_SIZE(GB)",
      SPACE "TOTAL_SIZE(GB)",
      SPACE - NVL(FREE_SPACE, 0) "USED_SIZE(GB)",
      ROUND(((SPACE - NVL(FREE_SPACE, 0)) / case when decode(MAX_SPACE,0,1,SPACE)=0 then 1000000000000 else decode(MAX_SPACE,0,1,SPACE) end ) * 100, 2)  "EXTEND_RATE(%)",
      ---ROUND(((SPACE - NVL(FREE_SPACE, 0)) / case when decode(SPACE,0,1,SPACE)=0 then 1000000000000 else decode(SPACE,0,1,SPACE) end ) * 100, 2) "PCT_USED_RATE(%)",
      round(((SPACE - NVL(FREE_SPACE, 0))/MAX_SPACE)*100,2)  "REAL_PERCENT(%)"
 FROM (SELECT TABLESPACE_NAME,
              SUM(MAX_SPACE) MAX_SPACE,
              SUM(SPACE) SPACE,
              SUM(BLOCKS) BLOCKS
         FROM (SELECT TABLESPACE_NAME,
                      ROUND(decode(AUTOEXTENSIBLE,
                                   'YES',
                                   SUM(MAXBYTES) / (1024 * 1024 * 1024),
                                   SUM(BYTES) / (1024 * 1024 * 1024)),
                            2) MAX_SPACE,
                      ROUND(SUM(BYTES) / (1024 * 1024 * 1024), 2) SPACE,
                      SUM(BLOCKS) BLOCKS
                 FROM DBA_DATA_FILES
                GROUP BY TABLESPACE_NAME, AUTOEXTENSIBLE)
        GROUP BY TABLESPACE_NAME) D,
      (SELECT TABLESPACE_NAME,
              ROUND(SUM(BYTES) / (1024 * 1024 * 1024), 2) FREE_SPACE
         FROM DBA_FREE_SPACE
        GROUP BY TABLESPACE_NAME) F,
        dba_tablespaces G
WHERE D.TABLESPACE_NAME = F.TABLESPACE_NAME(+)
AND D.TABLESPACE_NAME = G.TABLESPACE_NAME
--AND D.TABLESPACE_NAME like 'TBSP_DS_ODS_CRM'
order by 2  ;
prompt </TbsFree>


-- 无效对象 -- 
prompt <InvalidObj>
set line 200
SET FEEDBACK on
COLUMN owner           FORMAT a15             
COLUMN object_type     FORMAT a25             
COLUMN obj_count       FORMAT 999,999,999,999 
SELECT    owner  , object_type  , count(*) obj_count
FROM    dba_objects where   status='INVALID'   GROUP BY    owner  , object_type ORDER BY    owner  , object_type;
prompt </InvalidObj>

-- 无效索引 -- 
prompt <InvalidIdx>
set line 200
col owner for a15
col index_name for a30
col admin_option for a15
col status for a8;
col subpartition_name for a15
col partition_name for a15
col subname for a15
SET FEEDBACK on
SELECT owner,index_name,'' as subname ,status FROM dba_indexes where status = 'UNUSABLE'
union
SELECT INDEX_OWNER,index_name,partition_name,status FROM dba_ind_partitions where status = 'UNUSABLE'
union 
SELECT INDEX_OWNER,index_name,subpartition_name,status FROM dba_ind_subpartitions  where status = 'UNUSABLE';
prompt </InvalidIdx>

-- 无效约束 --
prompt <DISABLED_CONSTRAINTS>
COLUMN OWNER FORMAT a15  
COLUMN CONSTRAINT_NAME FORMAT a30  
COLUMN CONSTRAINT_TYPE FORMAT a15  
COLUMN TABLE_NAME FORMAT a30  
COLUMN R_OWNER FORMAT a15  
COLUMN R_CONSTRAINT_NAME FORMAT a30  
COLUMN DELETE_RULE FORMAT a15  
COLUMN STATUS FORMAT a10  
COLUMN DEFERRABLE FORMAT a10  
COLUMN DEFERRED FORMAT a10  
COLUMN VALIDATED FORMAT a10  
COLUMN GENERATED FORMAT a10  
COLUMN BAD FORMAT a5  
COLUMN RELY FORMAT a5  
COLUMN LAST_CHANGE FORMAT a20  
COLUMN INDEX_OWNER FORMAT a15  
COLUMN INDEX_NAME FORMAT a30  
COLUMN INVALID FORMAT a5  
COLUMN VIEW_RELATED FORMAT a10  
SELECT OWNER                    ,
CONSTRAINT_NAME          ,
CONSTRAINT_TYPE          ,
TABLE_NAME               ,
R_OWNER                  ,
R_CONSTRAINT_NAME        ,
DELETE_RULE              ,
STATUS                   ,
DEFERRABLE               ,
DEFERRED                 ,
VALIDATED                ,
GENERATED                ,
BAD                      ,
RELY                     ,
LAST_CHANGE              ,
INDEX_OWNER              ,
INDEX_NAME               ,
INVALID                  ,
VIEW_RELATED
FROM DBA_CONSTRAINTS
WHERE STATUS = 'DISABLED' 
AND OWNER not in
(select username
 from dba_users
 where default_tablespace in ('SYSTEM', 'SYSAUX')) ;
prompt </DISABLED_CONSTRAINTS>

-- 无效触发器 --
prompt <DISABLED_TRIGGERS>
COLUMN OWNER FORMAT A15  
COLUMN TRIGGER_NAME FORMAT A30  
COLUMN TRIGGER_TYPE FORMAT A20  
COLUMN TRIGGERING_EVENT FORMAT A30  
COLUMN TABLE_OWNER FORMAT A15  
COLUMN BASE_OBJECT_TYPE FORMAT A20  
COLUMN TABLE_NAME FORMAT A30  
COLUMN COLUMN_NAME FORMAT A30  
COLUMN REFERENCING_NAMES FORMAT A30  
COLUMN WHEN_CLAUSE FORMAT A40  
COLUMN STATUS FORMAT A10  
COLUMN DESCRIPTION FORMAT A50  
COLUMN ACTION_TYPE FORMAT A30  
SELECT                                                     
OWNER                             ,                   
TRIGGER_NAME                      ,                   
TRIGGER_TYPE                      ,                   
TRIGGERING_EVENT                  ,                   
TABLE_OWNER                       ,                   
BASE_OBJECT_TYPE                  ,                   
TABLE_NAME                        ,                   
COLUMN_NAME                       ,                   
REFERENCING_NAMES                 ,                   
WHEN_CLAUSE                       ,                   
STATUS                            ,                   
DESCRIPTION                       ,                   
ACTION_TYPE                                           
FROM DBA_TRIGGERS                                        
WHERE STATUS = 'DISABLED'                                 
AND OWNER not in                                        
(select username                                    
from dba_users                                   
where default_tablespace in ('SYSTEM', 'SYSAUX'));
prompt </DISABLED_TRIGGERS>

-- 数据库中超过3层的索引 --
prompt <IndexBle>
set pages 300
select 
owner,
table_name,
index_name,
BLEVEL
from dba_indexes 
where BLEVEL >3
and owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'ORDSYS', 
'ORDPLUGINS', 'SYSMAN','MDSYS', 'CTXSYS', 'AURORA$ORB$UNAUTHENTICATED', 'XDB','EXFSYS','FLOWS_030000','OLAPSYS','SCOTT','TSMSYS','WKSYS','WK_TEST','WMSYS','ORDDATA')
order by 1,2,3; 
prompt </IndexBle>

-- 表并行度检查 --
prompt <TABLEDEGREE>
set pages 3000
select table_name,DEGREE from dba_tables where DEGREE>'1';
prompt </TABLEDEGREE>

-- 索引并行度检查 --
prompt <INDEXESDEGREE>
set pages 3000
select index_name,DEGREE from DBA_INDEXES where DEGREE>'1';
prompt </INDEXESDEGREE>

-- 使用DBA角色的用户 --
prompt <DbaRole>
set pages 300
col GRANTEE for a20
col GRANTED_ROLE for a20
select GRANTEE,GRANTED_ROLE,ADMIN_OPTION,DEFAULT_ROLE from DBA_ROLE_PRIVS where GRANTED_ROLE='DBA' or GRANTED_ROLE='SYSDBA';
prompt </DbaRole>



-- 备份相关 --
prompt <backup>
SET LINESIZE 100
SET PAGESIZE 1000
set time on
set timing off
set head on
col STATUS format a23
col START_TIME format a15
col END_TIME format a15
col hrs format 999.99
select SESSION_KEY,INPUT_TYPE, STATUS, to_char(START_TIME, 'mm/dd/yy hh24:mi') start_time,to_char(END_TIME, 'mm/dd/yy hh24:mi') end_time,
elapsed_seconds / 3600 hrs
from V\$RMAN_BACKUP_JOB_DETAILS
order by session_key desc;
prompt </backup>
 
-----表碎片分析----------
prompt <frag>
set pagesize 0 linesize 200 echo off feedback off verify off  
col value new_val db_block_size noprint  
select value/1024 value from v\$parameter where name='db_block_size';   
set line 300  
set pagesize 300  
col table_name for a35  
col owner for a6  
col tab_size for 999999.999999  
col safe_space for 999999.999999  
select owner,table_name,blocks*&db_block_size/1024 TAB_SIZE,(AVG_ROW_LEN*NUM_ROWS+INI_TRANS*24)/(BLOCKS*&db_block_size*1024)*100 used_pct,((BLOCKS* &db_block_size*1024)-(AVG_ROW_LEN*NUM_ROWS+INI_TRANS*24))/1024/1024*0.9 safe_space   
from dba_tables  
where   
(BLOCKS IS NOT NULL AND BLOCKS>0)  
AND (&db_block_size IS NOT NULL AND &db_block_size>0)  
and blocks>1024*10  order by 4; 
prompt </frag>

---- 资源检查----
prompt <db_resource>
set pagesize 500 linesize 400
col inst_id for a10
col name for a20
col value for a20
col pct for a15
select to_char(inst_id) inst_id,
decode(resource_name,'processes','processes','sessions','sessions','parallel_max_servers','parallel_max_servers') name,
current_utilization,
max_utilization,
decode(limit_value, 'UNLIMITED', 'UNLIMITED', limit_value) value,
decode(max_utilization,
0,
0,
round(100 * current_utilization / limit_value, 2)) || '%' pct
from gv\$resource_limit
where resource_name in ('processes', 'sessions', 'parallel_max_servers')
union all
select '*',
'db_files',
(select count(*) from dba_data_files) ,
 null,
(select value from v\$parameter where name = 'db_files')  ,
round(100 * (select count(*) from dba_data_files) /
(select value from v\$parameter where name = 'db_files'),
2) || '%' 
from dual ;
prompt </db_resource>




---- redo日志切换频率明细--------
PROMPT <dbRedoswitch>
column dt format  a10  
column dy format a7  
column Total format 999  
column h0  format 99  
column h1  format 99  
column h2  format 99  
column h3  format 99  
column h4  format 99  
column h5  format 99  
 column h6  format 99  
 column h7  format 99  
 column h8  format 99  
 column h9  format 99  
 column h10  format 99  
 column h11  format 99  
 column h12  format 99  
 column h13  format 99  
 column h14  format 99  
 column h15  format 99  
 column h16  format 99  
 column h17  format 99  
 column h18  format 99  
 column h19  format 99  
 column h20  format 99  
 column h21  format 99  
 column h22  format 99  
 column h23  format 99  
   
 SELECT  to_char(first_time,'yyyy-mm-dd') Dt,  
         to_char(first_time, 'Dy') dy,  
         count(1) "Total",  
         SUM(decode(to_char(first_time, 'hh24'),'00',1,0)) "h0",  
         SUM(decode(to_char(first_time, 'hh24'),'01',1,0)) "h1",  
         SUM(decode(to_char(first_time, 'hh24'),'02',1,0)) "h2",  
         SUM(decode(to_char(first_time, 'hh24'),'03',1,0)) "h3",  
         SUM(decode(to_char(first_time, 'hh24'),'04',1,0)) "h4",  
         SUM(decode(to_char(first_time, 'hh24'),'05',1,0)) "h5",  
         SUM(decode(to_char(first_time, 'hh24'),'06',1,0)) "h6",  
         SUM(decode(to_char(first_time, 'hh24'),'07',1,0)) "h7",  
         SUM(decode(to_char(first_time, 'hh24'),'08',1,0)) "h8",  
         SUM(decode(to_char(first_time, 'hh24'),'09',1,0)) "h9",  
         SUM(decode(to_char(first_time, 'hh24'),'10',1,0)) "h10",  
         SUM(decode(to_char(first_time, 'hh24'),'11',1,0)) "h11",  
         SUM(decode(to_char(first_time, 'hh24'),'12',1,0)) "h12",  
         SUM(decode(to_char(first_time, 'hh24'),'13',1,0)) "h13",  
         SUM(decode(to_char(first_time, 'hh24'),'14',1,0)) "h14",  
         SUM(decode(to_char(first_time, 'hh24'),'15',1,0)) "h15",  
         SUM(decode(to_char(first_time, 'hh24'),'16',1,0)) "h16",  
         SUM(decode(to_char(first_time, 'hh24'),'17',1,0)) "h17",  
         SUM(decode(to_char(first_time, 'hh24'),'18',1,0)) "h18",  
         SUM(decode(to_char(first_time, 'hh24'),'19',1,0)) "h19",  
         SUM(decode(to_char(first_time, 'hh24'),'20',1,0)) "h20",  
         SUM(decode(to_char(first_time, 'hh24'),'21',1,0)) "h21",  
         SUM(decode(to_char(first_time, 'hh24'),'22',1,0)) "h22",  
         SUM(decode(to_char(first_time, 'hh24'),'23',1,0)) "h23"  
 FROM    V\$log_history  AlertErr1
  where first_time >= trunc(SYSDATE) - 18
 group by to_char(first_time,'yyyy-mm-dd') ,  
         to_char(first_time, 'Dy')  
 Order by 1 
/
PROMPT </dbRedoswitch>



-- 新增awr指标项  --
prompt <Top_10_SQL_statements_by_Elapsed_Time>
prompt <![CDATA[ 
select rownum as rank, a.*
  from (select PARSING_SCHEMA_NAME owner,
               SQL_FULLTEXT,
               elapsed_Time/1000/1000 elapsed_time,
               cpu_time/1000/1000 cpu_time,
               elapsed_Time/1000/1000 - cpu_time/1000/1000 wait_time,
               trunc((elapsed_Time - cpu_time) * 100 / elapsed_Time, 2) "wait_time_per%",
               executions,
               (elapsed_Time/1000/1000) / (executions + 1) Per_Time,
               buffer_gets,
               disk_reads,
               hash_value,
               USER_IO_WAIT_TIME/1000/1000 IO_WTIME,
               SORTS
          from v\$sqlarea t
         where elapsed_time/1000/1000 > 5 and PARSING_SCHEMA_NAME not in ('SYS','SYSTEM','ORACLE_OCM ','CTXSYS','APPQOSSYS','DBSNMP','SCOTT','OUTLN','QUEST','SYSMAN','ORDSYS','OLAPSYS','MDSYS','EXFSYS','XDB','CTXSYS','DMSYS','WMSYS')  
         order by elapsed_time desc) a
 where rownum < 11
 order by Per_Time desc;
prompt ]]>
prompt </Top_10_SQL_statements_by_Elapsed_Time>


prompt <Top_10_SQL_statements_by_Buffer_Gets>
prompt <![CDATA[ 
set lines 500
COLUMN disk_reads    FORMAT 999999999999999  
COLUMN buffer_gets   FORMAT 999999999999999  
COLUMN executions    FORMAT 999999999999999  
COLUMN exec_per_buffer FORMAT 9999999999999999
COLUMN hash_value    FORMAT 9999999999999999
COLUMN sql_text      FORMAT a60
SELECT *
  FROM (SELECT disk_reads,
               buffer_gets,
               executions,
               TRUNC(buffer_gets / executions) exec_per_buffer,
               hash_value,
               sql_text
          FROM v\$sqlarea
         WHERE executions > 0
         ORDER BY 2 DESC)
 WHERE ROWNUM < 5;
prompt ]]>
prompt </Top_10_SQL_statements_by_Buffer_Gets>

prompt <sql_statements_by_disk_reads>
prompt <![CDATA[ 
set lines 500
COLUMN disk_reads    FORMAT 999999999999999  
COLUMN buffer_gets   FORMAT 999999999999999  
COLUMN executions    FORMAT 999999999999999 
COLUMN reads_per_exec FORMAT 9999999999999999
COLUMN hash_value    FORMAT 9999999999999999
COLUMN sql_text      FORMAT a60
SELECT *
  FROM (SELECT disk_reads,
               buffer_gets,
               executions,
               TRUNC(buffer_gets / executions) reads_per_exec,
               hash_value,
               sql_text
          FROM v\$sqlarea
         WHERE executions > 0
         ORDER BY 1 DESC)
 WHERE ROWNUM < 11;
prompt ]]>
prompt </sql_statements_by_disk_reads>

-- 数据库内存命中率 --
--这条SQL语句使用了v$sysstat视图来获取数据库统计信息。它会返回几个关于数据库内存命中率的指标，包括"DB Block Gets - Total"
--（数据库块获取总数）、"Consistent Gets - Total"（一致性获取总数）、"DB Block Gets - Percentage"（数据库块获取百分比）、
--"DB Block Writes - Percentage"（数据库块写入百分比）、"DB Block Reads - Percentage"
--（数据库块读取百分比）以及"DB Block Writes - Percentage of Reads"（数据库块写入的百分比占读取的百分比）
prompt <Instance_Efficiency>
prompt <![CDATA[ 
SELECT
    (SELECT value FROM v\$sysstat WHERE name = 'db block gets') /
    (SELECT value FROM v\$sysstat WHERE name = 'db block gets') * 100 AS "DB Block Gets",
    (SELECT value FROM v\$sysstat WHERE name = 'consistent gets') /
    (SELECT value FROM v\$sysstat WHERE name = 'consistent gets') * 100 AS "Consistent Gets",
    (SELECT value FROM v\$sysstat WHERE name = 'db block gets') /
    (SELECT value FROM v\$sysstat WHERE name = 'consistent gets') * 100 AS "DB Block Gets",
    (SELECT value FROM v\$sysstat WHERE name = 'db block writes') /
    (SELECT value FROM v\$sysstat WHERE name = 'db block gets') * 100 AS "DB Block Writes",
    (SELECT value FROM v\$sysstat WHERE name = 'db block reads') /
    (SELECT value FROM v\$sysstat WHERE name = 'db block gets') * 100 AS "DB Block Reads",
    (SELECT value FROM v\$sysstat WHERE name = 'db block writes') /
    (SELECT value FROM v\$sysstat WHERE name = 'db block reads') * 100 AS "DB Block Writes"
FROM dual;
prompt ]]>
prompt </Instance_Efficiency>

-- 监控表空间的 I/O 比例 --
--f.phyrds pyr: 这是物理读块的数量，即从磁盘读取的数据块的数量。
--f.phyblkrd pbr: 这是物理块读的数量，即从单个数据块中读取的逻辑块的数量。
--f.phywrts pyw: 这是物理写转发的数量，即为了满足一次I/O请求而写入的块的数量。
--f.phyblkwrt pbw: 这是物理块写入的数量，即单个数据块中写入的逻辑块的数量的平均值。
prompt <Tablespace_IO_Stats> 
set lines 500
COLUMN name  FORMAT a15    
COLUMN "file" FORMAT a50
select df.tablespace_name name,
       df.file_name       "file",
       f.phyrds           pyr,
       f.phyblkrd         pbr,
       f.phywrts          pyw,
       f.phyblkwrt        pbw
  from v\$filestat f, dba_data_files df
 where f.file# = df.file_id
 order by df.tablespace_name;
prompt </Tablespace_IO_Stats>


prompt </datas>
spool off
exit
EOF


cat database_check_info.txt>>$db_file_name
${RMFILE} database_check_info.txt


 ##################shell##################
echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>">>$os_file_name
echo "<datas>">>$os_file_name
unamesr="`uname -sr`"
case "$unamesr" in
AIX*)
echo "<hostname1>">>$os_file_name
hostname>>$os_file_name
echo "</hostname1>">>$os_file_name


echo "<ProcessorType>">>$os_file_name
prtconf|grep "Processor Type">>$os_file_name
echo "</ProcessorType>">>$os_file_name

echo "<Uname1>">>$os_file_name
uname>>$os_file_name
oslevel -r>>$os_file_name
echo "</Uname1>">>$os_file_name


echo "<MemorySize1>">>$os_file_name
prtconf|grep "Memory Size"|grep -v Good|awk '{printf ("%d\n",$3/1024+0.5)}'>>$os_file_name
echo "</MemorySize1>">>$os_file_name


echo "<Pcocessors1>">>$os_file_name
prtconf|grep Processors>>$os_file_name
echo "</Pcocessors1>">>$os_file_name

echo "<swapinfo>">>$os_file_name
/usr/sbin/lsps -a>>$os_file_name
echo "</swapinfo>">>$os_file_name
;;
SunOS\ 5*)
echo "<hostname1>">>$os_file_name
hostname>>$os_file_name
echo "</hostname1>">>$os_file_name


echo "<Uname1>">>$os_file_name
uname -sr>>$os_file_name
echo "</Uname1>">>$os_file_name

echo "<MemorySize1>">>$os_file_name
/usr/sbin/prtconf | grep "Memory size" |awk '{printf ("%d\n",$3/1024+0.5)}'>>$os_file_name
echo "</MemorySize1>">>$os_file_name

echo "<Pcocessors1>">>$os_file_name
uname -X|grep NumCPU|awk '{printf ("%d\n",$3)}' >>$os_file_name
echo "</Pcocessors1>">>$os_file_name

echo "<swapinfo>">>$os_file_name
/usr/sbin/swap -s>>$os_file_name
echo "</swapinfo>">>$os_file_name
;;
HP*)
echo "<hostname1>">>$os_file_name
hostname>>$os_file_name
echo "</hostname1>">>$os_file_name

echo "<Oslevel>">>$os_file_name
uname -a>>$os_file_name
echo "</Oslevel>">>$os_file_name

echo "<Uname1>">>$os_file_name
uname -sr>>$os_file_name
echo "</Uname1>">>$os_file_name

echo "<sysmodel>">>$os_file_name
model>>$os_file_name
echo "</sysmodel>">>$os_file_name

echo "<MemorySize1>">>$os_file_name
echo "selall;info;wait;infolog;view;done" | /usr/sbin/cstm | grep "Total Configured Memory" | awk '{printf ("%d\n",$4/1024+0.5)}'>>$os_file_name
echo "</MemorySize1>">>$os_file_name

echo "<Pcocessors1>">>$os_file_name
sar -M 1 1|awk 'END {print NR-5}' >>$os_file_name
echo "</Pcocessors1>">>$os_file_name

echo "<swapinfo>">>$os_file_name
/usr/sbin/swapinfo -a>>$os_file_name
echo "</swapinfo>">>$os_file_name
;;
*)
echo "<hostname1>">>$os_file_name
hostname>>$os_file_name
echo "</hostname1>">>$os_file_name

echo "<Uname1>">>$os_file_name
uname -sr>>$os_file_name
echo "</Uname1>">>$os_file_name

echo "<uname_else>">>$os_file_name
uname -a>>$os_file_name
echo "</uname_else>">>$os_file_name

echo "<oslevel>">>$os_file_name
cat /etc/issue>>$os_file_name
echo "</oslevel>">>$os_file_name

echo "<pagesize_else>">>$os_file_name
getconf PAGE_SIZE>>$os_file_name
echo "</pagesize_else>">>$os_file_name

echo "<MemorySize1>">>$os_file_name
cat /proc/meminfo |grep MemTotal |awk '{printf ("%d\n",$2/1024/1024+0.5)}'>>$os_file_name
echo "</MemorySize1>">>$os_file_name

echo "<Pcocessors1>">>$os_file_name
cat /proc/cpuinfo |grep "processor"|wc -l>>$os_file_name
echo "</Pcocessors1>">>$os_file_name

echo "<swapinfo>">>$os_file_name 
/sbin/swapon -s>>$os_file_name
echo "</swapinfo>">>$os_file_name
;;
esac

####Check IP ADDRESS####################
os=`uname -a|awk '{print $1}'`
if [ $os = 'HP-UX' ];then
echo "<IpAddr1>">>$os_file_name 
netstat -in|grep -v lo|awk '{print $4}'|grep -v '[A-Za-z]'>>$os_file_name 
echo "</IpAddr1>">>$os_file_name

elif [ $os = 'AIX' ];then
echo "<IpAddr1>">>$os_file_name 
ifconfig -a|awk -vRS='inet' 'NR!=1{print $2}'|grep -v 127|grep -v :: >>$os_file_name 
echo "</IpAddr1>">>$os_file_name

elif [ $os = 'SunOS' ];then
echo "<IpAddr1>">>$os_file_name 
/usr/sbin/ifconfig -a|awk -vRS='inet' 'NR!=1{print $2}'|grep -v '[A-Za-z]'|grep -v 127|grep -v 0.0.0.0 >>$os_file_name 
echo "</IpAddr1>">>$os_file_name

else
echo "<IpAddr1>">>$os_file_name 
#/sbin/ifconfig | awk -vRS='inet addr:' 'NR!=1{print $1}' |grep -v 127 >>$os_file_name 
/sbin/ ifconfig -a | awk -vRS='inet ' 'NR!=1{print $1}' |grep -v 127”>>$os_file_name
echo "</IpAddr1>">>$os_file_name
fi

#######Check disk and CPU use##########

os=`uname -a|awk '{print $1}'`
if [ $os = 'HP-UX' ];then
echo "<Checkdisk1>">>$os_file_name 
bdf >>$os_file_name 
echo "</Checkdisk1>">>$os_file_name
echo "<Vmstatcheck1>">>$os_file_name 
vmstat 1 10 >>$os_file_name
echo "</Vmstatcheck1>">>$os_file_name

elif [ $os = 'AIX' ];then
echo "<Checkdisk1>">>$os_file_name 
df -k >>$os_file_name 
echo "</Checkdisk1>">>$os_file_name
echo "<Vmstatcheck1>">>$os_file_name 
vmstat 1 10 >>$os_file_name 
echo "</Vmstatcheck1>">>$os_file_name

elif [ $os = 'SunOS' ];then
echo "<Checkdisk1>">>$os_file_name 
df -k >>$os_file_name 
echo "</Checkdisk1>">>$os_file_name
echo "<Vmstatcheck1>">>$os_file_name 
vmstat 1 10 >>$os_file_name 
echo "</Vmstatcheck1>">>$os_file_name

else
echo "<Checkdisk1>">>$os_file_name 
df -h >>$os_file_name 
echo "</Checkdisk1>">>$os_file_name
echo "<Vmstatcheck1>">>$os_file_name 
vmstat 1 10 >>$os_file_name 
echo "</Vmstatcheck1>">>$os_file_name
fi

######check Memory#########
#####except Linux OS,Other OS not check yet######

os=`uname -a|awk '{print $1}'`
if [ $os = 'HP-UX' ];then
echo "<CheckMem1>">>$os_file_name 
vmstat 1 10 >>$os_file_name 
echo "</CheckMem1>">>$os_file_name

elif [ $os = 'AIX' ];then
echo "<CheckMem1>">>$os_file_name 
vmstat 1 10 >>$os_file_name 
echo "</CheckMem1>">>$os_file_name

elif [ $os = 'SunOS' ];then
echo "<CheckMem1>">>$os_file_name 
vmstat 1 10 >>$os_file_name 
echo "</CheckMem1>">>$os_file_name

else
echo "<CheckMem1>">>$os_file_name 
free -m >>$os_file_name 
echo "</CheckMem1>">>$os_file_name
fi


######check IO###########
os=`uname -a|awk '{print $1}'`
if [ $os = 'HP-UX' ];then
echo "<CheckIO1>">>$os_file_name 
iostat 2 3 >>$os_file_name 
echo "</CheckIO1>">>$os_file_name

elif [ $os = 'AIX' ];then
echo "<CheckIO1>">>$os_file_name 
iostat 2 3 >>$os_file_name 
echo "</CheckIO1>">>$os_file_name

elif [ $os = 'SunOS' ];then
echo "<CheckIO1>">>$os_file_name 
iostat 2 3 >>$os_file_name 
echo "</CheckIO1>">>$os_file_name

else
echo "<CheckIO1>">>$os_file_name 
iostat 2 3 >>$os_file_name 
echo "</CheckIO1>">>$os_file_name
fi


echo "<OpatchVersion1>">>$os_file_name
$ORACLE_HOME/OPatch/opatch lsinventory -oh $ORACLE_HOME >>$os_file_name
echo "</OpatchVersion1>">>$os_file_name

######check RAC crs status#########

cnt=`ps -ef |grep crsd.bin|grep -v grep|wc -l`
if [ $cnt = 1 ] ; then
CrsdPath=`ps -ef |grep crsd.bin|grep -v grep|awk '{print $(NF-1)}'`
CrsdbinPath=`dirname ${CrsdPath}`
ORA_CRS_HOME=`dirname ${CrsdbinPath}`

echo "<OlsnodesCrs>">>$os_file_name
$ORA_CRS_HOME/bin/olsnodes>>$os_file_name
echo "</OlsnodesCrs>">>$os_file_name

echo "<OcrcheckCrs>">>$os_file_name
$ORA_CRS_HOME/bin/ocrcheck>>$os_file_name
echo "</OcrcheckCrs>">>$os_file_name

echo "<CrsCheck>">>$os_file_name
$ORA_CRS_HOME/bin/crsctl check crs>>$os_file_name
echo "</CrsCheck>">>$os_file_name

echo "<CrsStat>">>$os_file_name
$ORA_CRS_HOME/bin/crsctl status res -t>>$os_file_name
echo "</CrsStat>">>$os_file_name

echo "<crsname>">>$os_file_name
$ORA_CRS_HOME/bin/cemutlo -n>>$os_file_name
echo "</crsname>">>$os_file_name

echo "<OcrConfig>">>$os_file_name
$ORA_CRS_HOME/bin/ocrconfig -showbackup>>$os_file_name
echo "</OcrConfig>">>$os_file_name
fi


#sqlplus -s "/ as sysdba"  <<EOF
#set pagesize
#col VALUE format a80
#spool alertpath.out
#select value from v\$diag_info where name='Diag Trace';
#select value from v\$parameter where name='background_dump_dest';
#spool off
#exit
#EOF
#ALERTPATHS=`cat alertpath.out|sed -e 's/[ ]*$//g'`
#instance_name=`cat alertpath.out|sed -n '2p'`
#
#echo "<AlertErr1>">>$os_file_name
#tail -2000 $ALERTPATHS/alert_$ORACLE_SID.log|grep ORA- >>$os_file_name
#echo "</AlertErr1>">>$os_file_name
#${RMFILE} alertpath.out



# 获取数据库版本
DB_VERSION=$(sqlplus -S /nolog << EOF
connect / as sysdba
set heading off;
select version from v\$instance;
exit;
EOF
)

# 根据数据库版本确定alert日志位置
if [[ $DB_VERSION == *"11."* ]]; then
sqlplus -s "/ as sysdba"  <<EOF
set pagesize
col VALUE format a80
spool alertpath.out
select value from v\$parameter where name='background_dump_dest';
spool off
exit
EOF
ALERTPATHS=`cat alertpath.out|sed -e 's/[ ]*$//g'`
instance_name=`cat alertpath.out|sed -n '2p'`

echo "<AlertErr1>">>$os_file_name
tail -2000 $ALERTPATHS/alert_$ORACLE_SID.log|grep ORA- >>$os_file_name
echo "</AlertErr1>">>$os_file_name
${RMFILE} alertpath.out
	
elif [[ $DB_VERSION == *"10."* ]]; then
   sqlplus -s "/ as sysdba"  <<EOF
set pagesize
col VALUE format a80
spool alertpath.out
select value from v\$parameter where name='background_dump_dest';
spool off
exit
EOF
ALERTPATHS=`cat alertpath.out|sed -e 's/[ ]*$//g'`
instance_name=`cat alertpath.out|sed -n '2p'`

echo "<AlertErr1>">>$os_file_name
tail -2000 $ALERTPATHS/alert_$ORACLE_SID.log|grep ORA- >>$os_file_name
echo "</AlertErr1>">>$os_file_name
${RMFILE} alertpath.out
	
else
    sqlplus -s "/ as sysdba"  <<EOF
set pagesize
col VALUE format a80
spool alertpath.out
select value from v\$diag_info where name='Diag Trace';
spool off
exit
EOF
ALERTPATHS=`cat alertpath.out|sed -e 's/[ ]*$//g'`
instance_name=`cat alertpath.out|sed -n '2p'`

echo "<AlertErr1>">>$os_file_name
tail -2000 $ALERTPATHS/alert_$ORACLE_SID.log|grep ORA- >>$os_file_name
echo "</AlertErr1>">>$os_file_name
${RMFILE} alertpath.out
fi



####### get_awr  #######################################

#sqlplus  "/ as sysdba" <<EOF
#@get_awr.sql
#exit
#EOF

#######Check cpu,memory ,io ,disk##########

echo "</datas>">>$os_file_name

 

  
# 定义文件名变量
generate_db_file_name() {
	echo "db_check_$(hostname)_$instance.xml"
}

generate_os_file_name() {
	echo "os_check_$(hostname)_$instance.xml"
}

# 存储所有历史文件名的字符串  
all_file_names=""  

# 检查新文件名是否在历史记录中
if [[ "${all_file_names[*]}" = "$db_file_name" || "${all_file_names[*]}" = "$os_file_name" ]]; then
	echo "文件名已存在，跳过打包"
else
	# 文件名不存在于历史记录中，进行打包操作
	tar -cvf "$health.tar.gz" "$db_file_name" "$os_file_name"
	# 将新文件名添加到历史记录字符串中  
    all_file_names="$all_file_names $db_file_name $os_file_name"  
fi

done; 
rm -rf db_check_*.xml os_check_*.xml

