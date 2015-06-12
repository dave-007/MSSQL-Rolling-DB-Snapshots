/*
PURPOSE: This procedure drops the oldest database snapshots for a given database,
retaining the most recent @snapshotsToKeep snapshots.
Includes a parameter option to respect active connections, so that long running queries can complete.
AUTHOR: David Cobb (sql@davidcobb.net)
SOURCE: https://github.com/dave-007/MSSQL-Rolling-DB-Snapshots
USAGE:
EXEC p_RemoveOldSnapshots 'DBName'
EXEC p_RemoveOldSnapshots @sourceDatabaseName='DBName', @snapshotsToKeep=5
EXEC p_RemoveOldSnapshots 'DBName', 5, 1
EXEC p_RemoveOldSnapshots @sourceDatabaseName='DBName', @snapshotsToKeep=5,@respectActiveConnections=1
EXEC p_RemoveOldSnapshots 'AdventureWorks2008R2',0  --removes them all

*/
USE master

IF EXISTS ( SELECT  0
            FROM    sys.procedures
            WHERE   NAME = 'p_RemoveOldSnapshots' )
    DROP PROC p_RemoveOldSnapshots
GO

CREATE PROC p_RemoveOldSnapshots
    (
      @sourceDatabaseName SYSNAME ,
      @snapshotsToKeep INT = 3 ,
      @respectActiveConnections BIT = 0
	)
AS
	SET NOCOUNT ON
    DECLARE @snapshotCount INT ,
        @snapshotsToRemove INT ,
        @SQL VARCHAR(MAX) ,
        @nextDatabaseSnapshot SYSNAME


    SELECT  @snapshotCount = COUNT(0)
    FROM    sys.databases
    WHERE   source_database_id = DB_ID(@sourceDatabaseName)

    SET @snapshotsToRemove = @snapshotCount - @snapshotsToKeep

    IF @snapshotsToRemove < 1
        BEGIN
            PRINT 'Only found ' + CONVERT(VARCHAR(5), @snapshotCount)
                + ' snapshots for ' + @sourceDatabaseName + ', keeping up to '
                + CONVERT(VARCHAR(5), @snapshotsToKeep)
                + ', so nothing was done. Exiting..'
        END
    ELSE
        BEGIN
            PRINT 'Found ' + CONVERT(VARCHAR(5), @snapshotCount)
                + ' snapshots, only keeping '
                + CONVERT(VARCHAR(5), @snapshotsToKeep)
                + ', so removing the oldest '
                + CONVERT(VARCHAR(5), @snapshotsToRemove) + ' snapshot(s):'
	--get the @snapshotsToRemove oldest snapshots
            SELECT TOP ( @snapshotsToRemove )
                    name
            INTO    #DatabaseSnapshotsToRemove
            FROM    sys.databases
            WHERE   source_database_id = DB_ID(@sourceDatabaseName)
            ORDER BY create_date

            SET @nextDatabaseSnapshot = 'init'
            WHILE @nextDatabaseSnapshot IS NOT NULL
                BEGIN
                    SELECT TOP 1
                            @nextDatabaseSnapshot = name
                    FROM    #DatabaseSnapshotsToRemove
                    ORDER BY NAME
		

                    IF @@ROWCOUNT = 1 --still database snapshots to drop
                        BEGIN
			--Check for active connections
                            IF EXISTS ( SELECT  0
                                        FROM    sys.sysprocesses
                                        WHERE   dbid = DB_ID(@nextDatabaseSnapshot) )
                                AND @respectActiveConnections = 1
                                ---give existing queries time to complete
                                BEGIN
                                    PRINT 'There are active connections to the database snapshot '
                                        + @nextDatabaseSnapshot
                                        + ' and @respectActiveConnections is on, so skipping this database.'
                                END
                            ELSE
                                BEGIN
                                    SET @SQL = 'DROP DATABASE ['
                                        + @nextDatabaseSnapshot + ']' 
                                    EXEC (@SQL)
                                    PRINT 'Dropped snapshot ' + @nextDatabaseSnapshot
                                END

                            DELETE  FROM #DatabaseSnapshotsToRemove
                            WHERE   name = @nextDatabaseSnapshot
                        END
                    ELSE
                        BEGIN
                            SET @nextDatabaseSnapshot = NULL
                        END

                END
            DROP TABLE #DatabaseSnapshotsToRemove
	
	--Verify results
            SELECT  @snapshotCount = COUNT(0)
            FROM    sys.databases
            WHERE   source_database_id = DB_ID(@sourceDatabaseName)

            PRINT 'There are now ' + CONVERT(VARCHAR(5), @snapshotCount)
                + ' snapshots of ' + @sourceDatabaseName + ' remaining.'
        END
	SET NOCOUNT OFF
GO


