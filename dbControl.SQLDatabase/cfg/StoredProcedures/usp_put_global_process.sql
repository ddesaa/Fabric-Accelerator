CREATE   PROCEDURE [cfg].[usp_put_global_process] @Global_Run_ID varchar(100), 
  @is_full_process varchar(20), 
  @trigger_name varchar(500) AS INSERT into cfg.GlobalProcess_lineage (
    global_run_id, Full_Process_Parameter, 
    Trigger_Name_Parameter
  ) 
values 
  (
    @Global_Run_ID, @is_full_process, 
    @trigger_name
  );
return @@IDENTITY

GO

