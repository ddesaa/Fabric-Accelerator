CREATE TABLE [cfg].[Environment_Setting] (
    [environment_key] BIGINT        IDENTITY (1, 1) NOT NULL,
    [workspace_id]    VARCHAR (500) NULL,
    [container_id]    VARCHAR (500) NULL,
    [container_name]  VARCHAR (500) NULL,
    [layer_name]      VARCHAR (500) NULL,
    PRIMARY KEY CLUSTERED ([environment_key] ASC)
);


GO

