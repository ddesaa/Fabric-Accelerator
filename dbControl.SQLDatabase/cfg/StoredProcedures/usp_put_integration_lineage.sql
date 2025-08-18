CREATE   PROCEDURE cfg.usp_put_integration_lineage
@global_run_id varchar(100),
@run_id varchar(100),
@sys_start_time datetime,
@sys_end_time datetime,
@log varchar(4000),
@process_step varchar(255),
@entity varchar(255)
AS
INSERT INTO cfg.Integration_lineage
([global_run_id]
,[run_id]
,[sys_start_time]
,[sys_end_time]
,[log]
,[process_step]
,[entity])
VALUES
(@global_run_id
,@run_id
,@sys_start_time
,@sys_end_time
,@log
,@process_step
,@entity)

GO

