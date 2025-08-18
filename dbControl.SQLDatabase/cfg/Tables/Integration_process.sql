CREATE TABLE [cfg].[Integration_process] (
    [integration_process_key] INT            IDENTITY (1, 1) NOT NULL,
    [trigger_name]            VARCHAR (255)  NOT NULL,
    [process_step]            VARCHAR (255)  NOT NULL,
    [entities]                VARCHAR (4000) NOT NULL,
    [process_order]           INT            NULL,
    [active]                  BIT            NULL,
    PRIMARY KEY CLUSTERED ([integration_process_key] ASC)
);


GO

