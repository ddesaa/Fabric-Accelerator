/******************************

Return the imported files to the landing zone by the ingestion process.

Author: Daniel De Saá

******************************/

create     proc [cfg].[usp_get_landing_files]
    @global_run_id varchar(200)
    , @entity VARCHAR(200)
as 
select 
    global_run_id
    ,il.entity
    ,json_value(il.log,'$.destination_folder') source_folder  
    ,json_value(il.log,'$.filename') filename
    ,json_value(il.log,'$.file_type') file_type
    ,json_value(e.integration_setting,'$.bronze_workspace_ID') bronze_workspace_id
    ,json_value(e.integration_setting,'$.bronze_workspace_ID') config_workspace_id
    ,json_value(e.integration_setting,'$.bronze_container') bronze_container
    ,json_value(e.integration_setting,'$.bronze_container') config_container
    ,json_value(e.integration_setting,'$.business_key') business_key
    ,json_value(e.integration_setting,'$.source_header') source_header
    
    ,'cfg/mapping' config_folder
from cfg.Integration_lineage il
inner join cfg.Integration_entities e on e.entity=il.entity
where il.process_step='landing'
and log is NOT null and log !='' 
and global_run_id=@global_run_id
and il.entity = @entity

GO

