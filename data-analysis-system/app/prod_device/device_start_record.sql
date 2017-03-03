DECLARE @APP_VERSION_CODE_1 INT = 9999;
DECLARE @APP_OP_TYPE INT = 1111;

IF OBJECT_ID('[Example].[TableName]', 'U') IS NOT NULL
	DROP TABLE [Example].[TableName];

CREATE TABLE [Example].[TableName] (
	TableRecorfdID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	DeviceID VARCHAR(255) NULL,
	StartTime DATETIME NULL,
	ChannelID INT NULL,
	VersionKey INT NULL,
	IP VARCHAR(20) NULL,
	Location NVARCHAR(255) NULL,
	LoadTime DATETIME NOT NULL
);
	
INSERT INTO [Example].[TableName]
(DeviceID, StartTime, ChannelID, VersionKey, IP, Location, LoadTime)
SELECT tb1.FDeviceID, tb1.StartTime, tb1.ChannelID, tb1.VersionCode, tb1.IP, NULL, GETDATE()
FROM (
	SELECT example_table.DeviceId
		END AS FDeviceID,
		CONVERT(
			DATETIME, 
			SUBSTRING(p.ParamValue, 1, 4) + '-' + 
			SUBSTRING(p.ParamValue, 5, 2) + '-' + 
			SUBSTRING(p.ParamValue, 7, 2) + ' ' + 
			SUBSTRING(p.ParamValue, 9, 2) + ':' + 
			SUBSTRING(p.ParamValue, 11, 2) + ':' + 
			SUBSTRING(p.ParamValue, 13, 2)
		) AS StartTime,
		example_table.ChannelName,
		ISNULL(dt.TrackerID, cn.ChannelID) AS ChannelID, example_table.VersionCode, example_table.ClientIP AS IP
	FROM   [1.1.1.1].[TableLogDB].[LGC].[tbHmcAppLog] DataWarehouse
	LEFT JOIN [1.1.1.1].[TableLogDB].[HIS].[tbHmcAppLogParam] p 
		ON p.LogId = example_table.LogId AND p.ParamKey = 'ActiveTime'
	LEFT JOIN [1.1.1.1].[TableLogDB].[DataWarehouse].[tbAppVersion] vs
		ON vs.VersionId = example_table.VersionCode
	LEFT JOIN [1.1.1.1].[TableLogDB].[DataWarehouse].[tbApp] app
		ON app.AppId = vs.AppId
	LEFT JOIN [1.1.1.1].[TableLogDB].[LGC].[tbChannelName] cn
		ON cn.ChannelName = example_table.ChannelName
	WHERE LEN(p.ParamValue) >= 17
	AND VersionCode >= @APP_VERSION_CODE_1
	AND example_table.OpTypeId = @APP_OP_TYPE
) tb1
GROUP BY tb1.FDeviceID, tb1.StartTime, tb1.ChannelID, tb1.VersionCode, tb1.IP
ORDER BY tb1.StartTime;

CREATE NONCLUSTERED INDEX IX_dbo_FactDeviceStartRecord_DeviceID
ON [Example].[TableName] (DeviceID);
CREATE NONCLUSTERED INDEX IX_dbo_FactDeviceStartRecord_StartTime
ON [Example].[TableName] (StartTime);
CREATE NONCLUSTERED INDEX IX_dbo_FactDeviceStartRecord_ChannelID
ON [Example].[TableName] (ChannelID);
CREATE NONCLUSTERED INDEX IX_dbo_FactDeviceStartRecord_VersionKey
ON [Example].[TableName] (VersionKey);
CREATE NONCLUSTERED INDEX IX_dbo_FactDeviceStartRecord_IP
ON [Example].[TableName] (IP);
