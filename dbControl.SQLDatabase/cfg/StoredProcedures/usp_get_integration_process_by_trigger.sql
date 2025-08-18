/******************************************
Get Process layers by trigger 
Order by Process_Order
Author: Daniel de Saá
*********************************************/
CREATE     PROCEDURE [cfg].[usp_get_integration_process_by_trigger]
@trigger_name varchar(255) = 'full_integration'
AS

SELECT
    ipss.integration_process_key,
    ipss.trigger_name,
    ipss.process_step
FROM 
    cfg.Integration_process ipss
WHERE 
    trigger_name = @trigger_name
    AND active=1
ORDER BY 
    ipss.process_order

GO

