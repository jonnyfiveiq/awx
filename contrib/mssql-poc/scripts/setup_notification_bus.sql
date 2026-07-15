-- Setup SQL Server notification bus for AWX dispatcherd
-- Replaces PostgreSQL pg_notify with polling table + Service Broker signal
-- Run against the 'awx' database on SQL Server

-- 1. Polling table: multi-consumer message log
--    Each consumer tracks its own last_seen_id, so multiple readers
--    (dispatcherd, cache_clear, wsrelay) all see every message.
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'awx_notify_messages')
CREATE TABLE awx_notify_messages (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    channel NVARCHAR(200) NOT NULL,
    payload NVARCHAR(MAX) NOT NULL,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_notify_id_channel')
CREATE INDEX IX_notify_id_channel ON awx_notify_messages (id, channel);

-- 2. Enable Service Broker on the awx database
ALTER DATABASE awx SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
ALTER DATABASE awx SET TRUSTWORTHY ON;

-- 3. Service Broker objects: lightweight wake-up signal
--    Publishers SEND a signal after INSERT; consumers WAITFOR RECEIVE
--    instead of busy-polling, giving near-instant notification delivery.
IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = 'AwxSignalMessage')
    CREATE MESSAGE TYPE [AwxSignalMessage] VALIDATION = NONE;

IF NOT EXISTS (SELECT * FROM sys.service_contracts WHERE name = 'AwxSignalContract')
    CREATE CONTRACT [AwxSignalContract] ([AwxSignalMessage] SENT BY INITIATOR);

IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = 'AwxSignalQueue')
    CREATE QUEUE [AwxSignalQueue] WITH STATUS = ON, RETENTION = OFF;

IF NOT EXISTS (SELECT * FROM sys.services WHERE name = 'AwxSignalService')
    CREATE SERVICE [AwxSignalService] ON QUEUE [AwxSignalQueue] ([AwxSignalContract]);
