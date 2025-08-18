/******************************

Return the imported entities to the landing zone by the ingestion process.

Author: Daniel De Saá

******************************/

create   proc cfg.usp_get_landing_entities
    @global_run_id varchar(200)
    
as 
select 
distinct
global_run_id
,entity

from cfg.Integration_lineage il
where il.process_step='landing'
and log is NOT null and log !='' 
and global_run_id=@global_run_id

GO

