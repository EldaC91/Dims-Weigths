WITH 

endicia_semana_mes AS (
    SELECT DISTINCT  
        [Postmark_Date] AS TranDate, 
        [Transaction_Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
		[Transaction_Amount] as DiscountedNetCharge
    FROM Endiciatransacprovision
    WHERE ISDATE([Transaction_Date (GMT)]) = 1
      AND CAST([Transaction_Date (GMT)] AS date) >= DATEADD(MONTH, -4, GETDATE())
    
    UNION ALL
    
    SELECT DISTINCT  
        [Postmark Date] AS TranDate, 
        [Transaction Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
		[Transaction Amount] as DiscountedNetCharge
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
        max(NetChargeAmount) as DiscountedNetCharge
    FROM 
        FEDEXHISTORICO
    GROUP BY 
        CAST(InvoiceDate AS date),
        CAST(InvoiceDate AS date),
        GroundTrackingIDPrefix + ExpressorGroundTrackingID

    UNION ALL
    -- UPS (CON PRIORIDAD PackageDimensions)
    SELECT 
    cast(TransactionDate as date), cast(InvoiceDate as date), TrackingNumber, sum(netamount) as DiscountedNetCharge
	FROM UPSNEWFORMAT
	GROUP BY 
	cast(TransactionDate as date), cast(InvoiceDate as date), TrackingNumber

    UNION ALL
    -- USPS (Endicia)
    SELECT DISTINCT 
        TranDate,
        InvDate,
        trackingnum,
        DiscountedNetCharge
    FROM endicia_semana_mes
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
	select  
		TranDate, InvDate, dw.trackingnum, mw.FulfillmentOrderNumber, dw.DiscountedNetCharge, 
		mw.Sku, mw.UomCode, mw.UomQuantity, mw.EstimatedShippingCost
	FROM Factura3meses dw
	INNER JOIN DWShippingInfo  mw
		ON dw.trackingnum = mw.trackingnum
	WHERE dw.trackingnum !=''
	and SalesOrderNumber in (select SalesOrderNumber from CantidadSkusxVenta)
	and cast(UomQuantity as float)/cast(Quantity as float)= 1
)

select InvDate,trackingnum, FulfillmentOrderNumber, sum(DiscountedNetCharge) as DiscountedNetCharge, Sku, UomCode, UomQuantity, 
max(EstimatedShippingCost) as EstimatedCost  
from PesosfacturaVentas
group by InvDate,trackingnum, FulfillmentOrderNumber, Sku, UomCode, UomQuantity
having InvDate >= DATEADD(WEEK, -2, GETDATE())
--AND FulfillmentOrderNumber = 'PO-259004529-EJD'