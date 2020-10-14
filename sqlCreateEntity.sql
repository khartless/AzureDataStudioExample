/*** Create the Entity table in MTech Data Warehouse ***/

DECLARE @startdate datetime 
SET @startdate = '1/1/16'

;with inventory_dates as 
(
    SELECT rtrim(b.FarmNo) + '-' + rtrim(b.EntityNo) AS ComplexEntityNo
    ,min(xDate) AS StartDate
    ,max(xDate) AS EndDate 
    FROM mtech.BimEntityInventory a 
    INNER JOIN mtech.mvProteinEntities b 
        ON a.ProteinEntitiesIRN = b.IRN
    INNER JOIN mtech.BimEntities c 
        ON a.ProteinEntitiesIRN = c.ProteinEntitiesIRN
    WHERE a.EntityHistoryFlag = 0
    AND AvgHatchDate >= @startdate
    AND a.xDate > '11/30/1899'
    Group by FarmNo, EntityNo
)

,trans_dates as 
(
    SELECT rtrim(b.FarmNo) + '-' + rtrim(b.EntityNo) AS ComplexEntityNo
    ,min(xDate) AS StartDate
    ,max(xDate) AS EndDate 
    FROM mtech.BimFieldTrans a 
    INNER JOIN mtech.mvProteinEntities b 
        ON a.ProteinEntitiesIRN = b.IRN
    INNER JOIN mtech.BimEntities c 
        ON a.ProteinEntitiesIRN = c.ProteinEntitiesIRN
    WHERE a.EntityHistoryFlag = 0
    AND AvgHatchDate >= @startdate
    AND a.xDate > '11/30/1899'
    Group by FarmNo, EntityNo
)

select 
a.ComplexEntityNo
,CASE WHEN a.StartDate < coalesce(b.StartDate, '11/30/3000') then a.StartDate else b.StartDate END AS StartDate
,CASE WHEN a.EndDate > coalesce(b.EndDate, '11/30/1899') THEN a.EndDate else b.EndDate END AS EndDate
FROM inventory_dates a 
LEFT JOIN trans_dates b 
ON a.ComplexEntityNo = b.ComplexEntityNo