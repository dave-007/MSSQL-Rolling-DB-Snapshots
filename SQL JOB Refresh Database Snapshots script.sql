/*
PURPOSE: Creates SQL Job with two steps. First step creates a new snapshot for a given database, second step removes old snapshots for a given database.
Schedule is for every minute, ADJUST SCHEDULE AS NEEDED!
See step contents for examples.
AUTHOR: David Cobb (sql@davidcobb.net)
SOURCE: https://github.com/dave-007/MSSQL-Rolling-DB-Snapshots
*/

USE [msdb]
GO

/****** Object:  Job [Refresh Rolling Database Snapshots]    Script Date: 05/27/2014 14:52:58 ******/
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'Refresh Rolling Database Snapshots')
EXEC msdb.dbo.sp_delete_job @job_name=N'Refresh Rolling Database Snapshots', @delete_unused_schedule=1
GO

USE [msdb]
GO

/****** Object:  Job [Refresh Rolling Database Snapshots]    Script Date: 05/27/2014 14:52:58 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]]    Script Date: 05/27/2014 14:52:58 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Refresh Rolling Database Snapshots', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'DESCRIPTION:
First step creates a new snapshot for a given database, second step removes old snapshots for a given database.

See step contents for examples.

To determine the latest snapshot for a given database, use the function dbo.fn_GetLatestSnapshot in this syntax:
SELECT dbo.fn_GetLatestSnapshot(''DBName'')

AUTHOR: David Cobb (sql@davidcobb.net)
SOURCE: https://github.com/dave-007/MSSQL-Rolling-DB-Snapshots
', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Create database snapshot(s)]    Script Date: 05/27/2014 14:52:58 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Create database snapshot(s)', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC p_CreateTimeStampedDatabaseSnapshot ''AdventureWorks2008R2''
--additional snapshots may be created for other databases using the same syntax
-- EXEC p_CreateTimeStampedDatabaseSnapshot ''dbname''''', 
		@database_name=N'master', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Remove oldest snapshot(s)]    Script Date: 05/27/2014 14:52:59 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Remove oldest snapshot(s)', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'--remove the oldest snapshots, keeping 5, but don''t remove them if there are open queries
EXEC p_RemoveOldSnapshots @sourceDatabaseName = ''AdventureWorks2008R2'', @snapshotsToKeep = 5, @respectActiveConnections = 1
--ex. additional snapshots may be removed for other databases using the same syntax
--EXEC p_RemoveOldSnapshots @sourceDatabaseName = ''dbName'' , @snapshotsToKeep = 5
--ex. also accepts positional parameters
--EXEC  p_RemoveOldSnapshots ''dbName'' ,  5

--ex. remove the oldest snapshots, keeping 10, even if there are queries open in the database
--EXEC p_RemoveOldSnapshots @sourceDatabaseName = '''', @snapshotsToKeep = 10,  @respectActiveConnections = 0', 
		@database_name=N'master', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every 1 minutes', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20140527, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'5538a0a1-4285-4bec-8087-4789f64543c6'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO


