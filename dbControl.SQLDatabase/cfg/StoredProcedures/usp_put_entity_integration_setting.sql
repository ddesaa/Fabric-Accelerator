
/*****************************************************
Set config of entity to the integration process  
Author: Daniel de Saá
*******************************************************/
create   proc cfg.usp_put_entity_integration_setting
 @entity VARCHAR(200) 										
 ,@source VARCHAR(200) = Null									
 ,@source_query varchar(8000) = Null				
 ,@source_file_type VARCHAR(200) = Null							
 ,@source_name VARCHAR(500) = Null 						
 ,@source_folder VARCHAR(500)  = Null
 ,@target_folder VARCHAR(500)  = Null
 ,@bronze_layer VARCHAR(500) = Null
 ,@trigger_name VARCHAR(500) = Null
 ,@filename VARCHAR(500) =  Null
 ,@business_key VARCHAR(800) = Null
 ,@silver_layer VARCHAR(500) = Null
 ,@gold_layer VARCHAR(500) = Null
 ,@silver_notebook VARCHAR(500) = Null
 ,@gold_notebook VARCHAR(500) = Null
 ,@header varchar(5) = NULL
 as

declare @bronze_workspace_id varchar(500)
declare @silver_workspace_id varchar(500)
declare @gold_workspace_id varchar(500)
declare @source_container varchar(500)
declare @bronze_container varchar(500)
declare @silver_container varchar(500)
declare @gold_container varchar(500)
declare @silver_notebook_id varchar(500)
declare @gold_notebook_id varchar(500)
declare @tbl table (entity varchar(200),integration_setting varchar(8000))


select @bronze_workspace_id=workspace_id,@bronze_container=container_id from [cfg].[Environment_Setting] where layer_name=@bronze_layer
select @silver_workspace_id=workspace_id,@silver_container=container_id from [cfg].[Environment_Setting] where layer_name=@silver_layer
select @gold_workspace_id=workspace_id,@gold_container=container_id from [cfg].[Environment_Setting] where layer_name=@gold_layer
select @source_container=container_id from [cfg].[Environment_Setting] where layer_name=@Source_name 
select  @silver_notebook_id=notebook_id from [cfg].[Notebooks_Setting] where notebook_name=@silver_notebook  
select  @gold_notebook_id=notebook_id from [cfg].[Notebooks_Setting] where notebook_name=@gold_notebook  
insert into @tbl 
SELECT 
	@entity AS entity
	,(
		SELECT
		@source as source 
		,@source_folder AS source_folder
		,@bronze_workspace_id as bronze_workspace_ID
		,@silver_workspace_id as silver_workspace_ID
		,@gold_workspace_id as gold_workspace_ID
		,@source_container as source_container
		,@bronze_container as bronze_container
		,@silver_container as silver_container
		,@gold_container as gold_container
		,@header as source_header
		,@bronze_layer as bronze_layer
		,@silver_layer as silver_layer
		,@gold_layer as gold_layer
		,@entity AS entity
		,@business_key as business_key
        ,@source_file_type AS file_type
		,@target_folder as destination_folder
        ,@trigger_name AS trigger_name
		,@filename  AS filename
		,@source_query as source_query
		,@silver_notebook_id as silver_notebook
		,@gold_notebook_id as gold_notebook
		
		FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
	) AS integration_setting;


MERGE cfg.Integration_entities AS tgt
USING @tbl AS src
ON tgt.entity = src.entity 
WHEN MATCHED THEN
UPDATE SET tgt.integration_setting = src.integration_setting 
WHEN NOT MATCHED THEN
INSERT (entity, integration_setting) 
VALUES (src.entity , src.integration_setting);

select * from cfg.Integration_entities where entity=@entity

GO

