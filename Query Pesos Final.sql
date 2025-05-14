WITH BOXFECHAS AS (
    SELECT 
        TRACKING_NUMBER,
        OrderDate,
        WEIGHT AS WeightBox,
        CASE WHEN LEN(DIMENSIONS) >= 5 THEN CAST(SUBSTRING(DIMENSIONS, 1, CHARINDEX('x', DIMENSIONS) - 1) AS FLOAT) ELSE 0.00 END AS DimHeightBOX,
        CASE WHEN LEN(DIMENSIONS) >= 5 THEN CAST(SUBSTRING(DIMENSIONS, CHARINDEX('x', DIMENSIONS) + 1, CHARINDEX('x', DIMENSIONS, CHARINDEX('x', DIMENSIONS) + 1) - CHARINDEX('x', DIMENSIONS) - 1) AS FLOAT) ELSE 0.00 END AS DimLengthBOX,
        CASE WHEN LEN(DIMENSIONS) >= 5 THEN CAST(RIGHT(DIMENSIONS, LEN(DIMENSIONS) - CHARINDEX('x', DIMENSIONS, CHARINDEX('x', DIMENSIONS) + 1)) AS FLOAT) ELSE 0.00 END AS DimWidthBOX,
        CCN_ORDER_NUMBER
    FROM BOX
    WHERE ISDATE(OrderDate) = 1
),

endicia_semana_mes AS (
    SELECT DISTINCT  
        [Postmark_Date] AS TranDate, 
        [Transaction_Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
        [Weight_(lb)], 
        [Package Height], 
        [Package Length], 
        [Package Width]
    FROM Endiciatransacprovision
    WHERE ISDATE([Transaction_Date (GMT)]) = 1
      AND CAST([Transaction_Date (GMT)] AS date) >= DATEADD(MONTH, -4, GETDATE())
    
    UNION ALL
    
    SELECT DISTINCT  
        [Postmark Date] AS TranDate, 
        [Transaction Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
        [Weight (lb)], 
        [Package Height], 
        [Package Length], 
        [Package Width]
    FROM endicia_semanal
    WHERE ISDATE([Transaction Date (GMT)]) = 1
      AND CAST([Transaction Date (GMT)] AS date) >= DATEADD(MONTH, -2, GETDATE())
),

Factura3meses AS (

    -- FEDEX
    SELECT DISTINCT
        CAST(InvoiceDate AS date) AS TranDate,
        CAST(InvoiceDate AS date) AS InvDate,
        GroundTrackingIDPrefix + ExpressorGroundTrackingID AS trackingnum,
        MAX(ActualWeightAmount) AS max_weight, 
        COALESCE(NULLIF(DimHeight, ''), CAST(DimHeightBOX AS VARCHAR)) AS DimHeight,
        COALESCE(NULLIF(DimLength, ''), CAST(DimLengthBOX AS VARCHAR)) AS DimLength,
        COALESCE(NULLIF(DimWidth, ''), CAST(DimWidthBOX AS VARCHAR)) AS DimWidth,
        MAX(CCN_ORDER_NUMBER) AS CCN_ORDER_NUMBER
    FROM 
        FEDEXHISTORICO
    LEFT JOIN 
        BOXFECHAS ON GroundTrackingIDPrefix + ExpressorGroundTrackingID = TRACKING_NUMBER
    WHERE 
        CAST(InvoiceDate AS date) >= DATEADD(MONTH, -3, GETDATE())
    GROUP BY 
        CAST(InvoiceDate AS date),
        CAST(InvoiceDate AS date),
        GroundTrackingIDPrefix + ExpressorGroundTrackingID,
        COALESCE(NULLIF(DimHeight, ''), CAST(DimHeightBOX AS VARCHAR)),
        COALESCE(NULLIF(DimLength, ''), CAST(DimLengthBOX AS VARCHAR)),
        COALESCE(NULLIF(DimWidth, ''), CAST(DimWidthBOX AS VARCHAR))

    UNION ALL

    -- UPS (CON PRIORIDAD PackageDimensions)
    SELECT 
    TranDate,
    InvDate,
    TrackingNumber,
    max_weight,
    Valor1 AS DimHeight,
    Valor2 AS DimLength,
    Valor3 AS DimWidth,
    CCN_ORDER_NUMBER
	FROM (
    SELECT 
        CAST(u.TransactionDate AS date) AS TranDate,
        CAST(u.InvoiceDate AS date) AS InvDate,
        u.TrackingNumber,
        CASE 
            WHEN u.EnteredWeight != 0 THEN u.EnteredWeight
            WHEN u.BilledWeight IS NOT NULL AND u.BilledWeight != 0 THEN u.BilledWeight
            ELSE b.WeightBox
        END AS max_weight,
        CASE 
            WHEN COALESCE(u.PackageDimensions, '') <> '' THEN CAST(SUBSTRING(u.PackageDimensions, 1, CHARINDEX('x', u.PackageDimensions) - 1) AS FLOAT)
            WHEN LEN(u.DetailKeyedDim) >= 5 THEN CAST(SUBSTRING(u.DetailKeyedDim, 1, CHARINDEX('x', u.DetailKeyedDim) - 1) AS FLOAT)
            ELSE b.DimHeightBOX
        END AS Valor1,
        CASE 
            WHEN COALESCE(u.PackageDimensions, '') <> '' THEN CAST(SUBSTRING(u.PackageDimensions, CHARINDEX('x', u.PackageDimensions) + 1, CHARINDEX('x', u.PackageDimensions, CHARINDEX('x', u.PackageDimensions) + 1) - CHARINDEX('x', u.PackageDimensions) - 1) AS FLOAT)
            WHEN LEN(u.DetailKeyedDim) >= 5 THEN CAST(SUBSTRING(u.DetailKeyedDim, CHARINDEX('x', u.DetailKeyedDim) + 1, CHARINDEX('x', u.DetailKeyedDim, CHARINDEX('x', u.DetailKeyedDim) + 1) - CHARINDEX('x', u.DetailKeyedDim) - 1) AS FLOAT)
            ELSE b.DimLengthBOX
        END AS Valor2,
        CASE 
            WHEN COALESCE(u.PackageDimensions, '') <> '' THEN CAST(RIGHT(u.PackageDimensions, LEN(u.PackageDimensions) - CHARINDEX('x', u.PackageDimensions, CHARINDEX('x', u.PackageDimensions) + 1)) AS FLOAT)
            WHEN LEN(u.DetailKeyedDim) >= 5 THEN CAST(RIGHT(u.DetailKeyedDim, LEN(u.DetailKeyedDim) - CHARINDEX('x', u.DetailKeyedDim, CHARINDEX('x', u.DetailKeyedDim) + 1)) AS FLOAT)
            ELSE b.DimWidthBOX
        END AS Valor3,
        CCN_ORDER_NUMBER,
        ROW_NUMBER() OVER (
            PARTITION BY u.TrackingNumber
            ORDER BY 
                CASE 
                    WHEN COALESCE(u.PackageDimensions, '') <> '' THEN 1
                    WHEN LEN(u.DetailKeyedDim) >= 5 THEN 2
                    ELSE 3
                END ASC,
                u.InvoiceDate DESC
        ) AS RowNum
    FROM 
        UPSNEWFORMAT u
    LEFT JOIN 
        BOXFECHAS b ON u.TrackingNumber = b.TRACKING_NUMBER
    WHERE 
        CAST(u.InvoiceDate AS date) >= DATEADD(MONTH, -3, GETDATE())
	) AS UPSFiltrado
	WHERE RowNum = 1


    UNION ALL

    -- USPS (Endicia)
    SELECT DISTINCT 
        TranDate,
        InvDate,
        trackingnum,
        COALESCE([Weight_(lb)], b.WeightBox) AS max_weight,
        COALESCE([Package Height], b.DimHeightBOX) AS DimHeight,
        COALESCE([Package Length], b.DimLengthBOX) AS DimLength,
        COALESCE([Package Width], b.DimWidthBOX) AS DimWidth,
        CCN_ORDER_NUMBER
    FROM 
        endicia_semana_mes e
    LEFT JOIN 
        BOXFECHAS b ON b.TRACKING_NUMBER = e.trackingnum
    WHERE 
        trackingnum IS NOT NULL
        AND CAST(InvDate AS date) >= DATEADD(MONTH, -4, GETDATE()) -- USPS 4 meses porque vienen mensual
),

DWVentasUnicas as (
	Select 
		SalesOrderNumber, Sku
	from DWShippingInfo as dws
	where cast(SalesOrderDate as date ) between dateadd(MONTH,-4, cast(GETDATE() as date)) and cast(GETDATE() as date)
	group by SalesOrderNumber, Sku
),

CantidadSkusxVenta as (
	select
		SalesOrderNumber, count(Sku) CantidadSkus
	from DWVentasUnicas dw 
	group by SalesOrderNumber
	having count(Sku) =1
),

PesosfacturaVentas as (
	--Lista los trackings con su peso y le coloca la moda y las frecuencia con que ese peso fue facturado
	select  
		TranDate, InvDate, dw.trackingnum, max_weight, DimHeight, DimLength, DimWidth, mw.Sku, mw.UomCode, mw.UomQuantity
	FROM Factura3meses dw
	INNER JOIN DWShippingInfo  mw
		ON dw.trackingnum = mw.trackingnum
	WHERE dw.trackingnum !=''
	and SalesOrderNumber in (select SalesOrderNumber from CantidadSkusxVenta)
	and cast(UomQuantity as float)/cast(Quantity as float)= 1
),

RankedData AS (
    SELECT 
        TranDate, 
        InvDate, 
        trackingnum, 
        Sku, 
        UomCode, 
        UomQuantity,
        max_weight, 
        DimHeight, 
        DimLength, 
        DimWidth,
        RANK() OVER (
            PARTITION BY TranDate, trackingnum, Sku, UomCode 
            ORDER BY InvDate DESC
        ) AS Ranking
    FROM PesosfacturaVentas
),

Max_Rank as (SELECT 
    TranDate, 
    InvDate, 
    trackingnum, 
    Sku, 
    UomCode, 
    UomQuantity,
    MAX(max_weight) AS Weight, 
    MAX(DimHeight) AS Height, 
    MAX(DimLength) AS Length, 
    MAX(DimWidth) AS Width,
    Ranking
FROM RankedData
GROUP BY TranDate, InvDate, trackingnum, Sku, UomCode, UomQuantity, Ranking
),

EJD_DIMS as (SELECT aa.*, 
       bb.[EJD Weight], 
       bb.[EJD Height], 
       bb.[EJD Length], 
       bb.[EJD Width],
       bb.[BaseUnit]
FROM Max_Rank aa
LEFT JOIN (
    SELECT DISTINCT [EVP SKU], [EVP UOM CODE], [EVP UOM QTY], [EJD Weight], [EJD Height], [EJD Length], [EJD Width], [BaseUnit]
    FROM [stg-MaxWarehouse].[EvpCsv].[EVPAlllSKUsDimNWeight]
) bb
ON aa.Sku COLLATE SQL_Latin1_General_CP1_CI_AS = bb.[EVP SKU] COLLATE SQL_Latin1_General_CP1_CI_AS
and aa.UomCode COLLATE SQL_Latin1_General_CP1_CI_AS= bb.[EVP UOM CODE] COLLATE SQL_Latin1_General_CP1_CI_AS
),

Ventas as (
    SELECT * 
    FROM [DWH-MaxWarehouse].[Fact].[Sales]
    WHERE SalesOrderDateTime >= DATEADD(MONTH, -4, CAST(GETDATE() AS DATETIME))
      AND SalesOrderDateTime < DATEADD(DAY, 1, CAST(GETDATE() AS DATETIME))
)

select distinct * from EJD_DIMS 
--where trackingnum = '1Z53E7100300373422'


--select distinct aa.*, bb.SalesStatusEVP, bb.TotalSales, bb.TotalCost from EJD_DIMS aa
--LEFT JOIN  Ventas bb
--ON aa.trackingnum COLLATE SQL_Latin1_General_CP1_CI_AS = bb.TrackingNumber COLLATE SQL_Latin1_General_CP1_CI_AS
--and aa.Sku COLLATE SQL_Latin1_General_CP1_CI_AS = bb.ParentSku COLLATE SQL_Latin1_General_CP1_CI_AS

