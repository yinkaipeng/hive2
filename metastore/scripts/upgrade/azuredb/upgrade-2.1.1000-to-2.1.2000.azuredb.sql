SELECT 'Upgrading MetaStore schema from 2.1.1000 to 2.1.2000' AS MESSAGE;

/****** Add column for bit vector in PART_COL_STATS ******/
ALTER TABLE [dbo].[PART_COL_STATS] ADD [BIT_VECTOR] [varbinary](max) NULL;
ALTER TABLE [dbo].[TAB_COL_STATS] ADD [BIT_VECTOR] [varbinary](max) NULL;

UPDATE [dbo].[VERSION] SET SCHEMA_VERSION='2.1.2000', VERSION_COMMENT='Hive release version 2.1.2000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.1000 to 2.1.2000' AS MESSAGE;
