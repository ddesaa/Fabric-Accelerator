
/*********************************************
Return the entity configuration of a process layer
Author: Daniel de Saá
*********************************************/
CREATE      PROCEDURE [cfg].[usp_get_integration_settings] 
  @integration_key int = 3, 
  @is_full_process varchar(10) = 'Yes' 
AS 
SELECT 
  integration_process_key, 
  cfg.entity, 
  cfg.integration_setting, 
  CASE WHEN @is_full_process in ('Yes', 'Si') THEN '1900-01-01' ELSE coalesce(
    convert(
      varchar(10), 
      lp.Last_Process, 
      120
    ), 
    '1900-01-01'
  ) END last_process, 
  'Argentina Standard Time' datetime_region 
FROM 
  cfg.Integration_process ipr CROSS APPLY string_split(ipr.entities, ',') en 
  INNER JOIN cfg.Integration_entities cfg ON cfg.entity = en.value 
  LEFT JOIN (
    SELECT 
      entity, 
      MAX(SYS_start_time) last_process 
    FROM 
      cfg.Integration_lineage 
    WHERE 
      process_step = 'landing' 
    GROUP BY 
      entity
  ) lp ON lp.entity = cfg.entity 
WHERE 
  integration_process_key = @Integration_key 
  and (ipr.process_step in ('landing','bronze') 
    or (ipr.process_step='silver' and JSON_VALUE(cfg.integration_setting,'$.silver_notebook') is not null)
    or (ipr.process_step='gold' and JSON_VALUE(cfg.integration_setting,'$.gold_notebook') is not null)
    )
  AND active = 1

GO

