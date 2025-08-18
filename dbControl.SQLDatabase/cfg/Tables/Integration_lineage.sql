CREATE TABLE [cfg].[Integration_lineage] (
    [integration_lineage_key] BIGINT         IDENTITY (1, 1) NOT NULL,
    [global_run_id]           VARCHAR (100)  NULL,
    [run_id]                  VARCHAR (100)  NULL,
    [sys_start_time]          DATETIME       NULL,
    [sys_end_time]            DATETIME       NULL,
    [log]                     VARCHAR (4000) NULL,
    [process_step]            VARCHAR (255)  NULL,
    [entity]                  VARCHAR (255)  NULL,
    PRIMARY KEY CLUSTERED ([integration_lineage_key] ASC)
);


GO

