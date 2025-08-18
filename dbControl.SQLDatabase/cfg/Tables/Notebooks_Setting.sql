CREATE TABLE [cfg].[Notebooks_Setting] (
    [notebook_key]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [notebook_id]   VARCHAR (500) NULL,
    [notebook_name] VARCHAR (500) NULL,
    PRIMARY KEY CLUSTERED ([notebook_key] ASC)
);


GO

