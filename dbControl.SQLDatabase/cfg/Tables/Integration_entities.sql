CREATE TABLE [cfg].[Integration_entities] (
    [entity_key]          INT            IDENTITY (1, 1) NOT NULL,
    [entity]              VARCHAR (500)  NOT NULL,
    [integration_setting] VARCHAR (8000) NULL,
    PRIMARY KEY CLUSTERED ([entity_key] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [idx_integration_entities_unique]
    ON [cfg].[Integration_entities]([entity] ASC);


GO

