-- Setup SQL Server database and notification bus for EDA (Event-Driven Ansible)
-- Replaces PostgreSQL pg_notify with polling table + Service Broker signal
-- Run against the SQL Server instance (master database initially, then USE eda)
-- ANSTRAT-1887 Phase 3+4: EDA MSSQL Migration

-- 1. Create the EDA database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'eda')
    CREATE DATABASE eda;
GO

USE eda;
GO

-- 2. Polling table: multi-consumer message log
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'awx_notify_messages')
CREATE TABLE awx_notify_messages (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    channel NVARCHAR(200) NOT NULL,
    payload NVARCHAR(MAX) NOT NULL,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_notify_id_channel')
CREATE INDEX IX_notify_id_channel ON awx_notify_messages (id, channel);

-- 3. Enable Service Broker on the eda database
ALTER DATABASE eda SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
ALTER DATABASE eda SET TRUSTWORTHY ON;

-- 4. Service Broker objects: lightweight wake-up signal
IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = 'AwxSignalMessage')
    CREATE MESSAGE TYPE [AwxSignalMessage] VALIDATION = NONE;

IF NOT EXISTS (SELECT * FROM sys.service_contracts WHERE name = 'AwxSignalContract')
    CREATE CONTRACT [AwxSignalContract] ([AwxSignalMessage] SENT BY INITIATOR);

IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = 'AwxSignalQueue')
    CREATE QUEUE [AwxSignalQueue] WITH STATUS = ON, RETENTION = OFF;

IF NOT EXISTS (SELECT * FROM sys.services WHERE name = 'AwxSignalService')
    CREATE SERVICE [AwxSignalService] ON QUEUE [AwxSignalQueue] ([AwxSignalContract]);
