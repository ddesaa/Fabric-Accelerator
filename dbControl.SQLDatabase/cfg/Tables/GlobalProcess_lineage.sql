CREATE TABLE [cfg].[GlobalProcess_lineage] (
    [GlobalProcess_lineage_key] BIGINT        IDENTITY (1, 1) NOT NULL,
    [global_run_id]             VARCHAR (100) NULL,
    [Full_Process_Parameter]    VARCHAR (100) NULL,
    [Trigger_Name_Parameter]    VARCHAR (100) NULL,
    PRIMARY KEY CLUSTERED ([GlobalProcess_lineage_key] ASC)
);


GO

