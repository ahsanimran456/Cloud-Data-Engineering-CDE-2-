-- first question query 
SELECT TOP 5 
    c.CustomerID,
    c.Name AS CustomerName,
    SUM(o.TotalAmount) AS TotalSpent
FROM Customer c
JOIN SalesOrder o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.Name
ORDER BY TotalSpent DESC;

-- SECOND question query 
SELECT 
    s.SupplierID,
    s.Name AS SupplierName,
    COUNT(DISTINCT pod.ProductID) AS ProductCount
FROM Supplier s
JOIN PurchaseOrder po ON s.SupplierID = po.SupplierID
JOIN PurchaseOrderDetail pod ON po.OrderID = pod.OrderID
GROUP BY s.SupplierID, s.Name
HAVING COUNT(DISTINCT pod.ProductID) > 10;


-- 3
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    SUM(sod.Quantity) AS TotalOrderedQty
FROM Product p
JOIN SalesOrderDetail sod ON p.ProductID = sod.ProductID
WHERE p.ProductID NOT IN (
    SELECT rd.ProductID 
    FROM ReturnDetail rd
)
GROUP BY p.ProductID, p.Name;

-- 4

SELECT 
    c.CategoryID,
    c.Name AS CategoryName,
    p.Name AS ProductName,
    p.Price
FROM Product p
JOIN Category c ON p.CategoryID = c.CategoryID
WHERE p.Price = (
    SELECT MAX(p2.Price)
    FROM Product p2
    WHERE p2.CategoryID = c.CategoryID
);


-- 5
SELECT 
    so.OrderID,
    c.Name AS CustomerName,
    p.Name AS ProductName,
    cat.Name AS CategoryName,
    s.Name AS SupplierName,
    sod.Quantity
FROM SalesOrder so
JOIN Customer c ON so.CustomerID = c.CustomerID
JOIN SalesOrderDetail sod ON so.OrderID = sod.OrderID
JOIN Product p ON sod.ProductID = p.ProductID
JOIN Category cat ON p.CategoryID = cat.CategoryID
JOIN PurchaseOrderDetail pod ON p.ProductID = pod.ProductID
JOIN PurchaseOrder po ON pod.OrderID = po.OrderID
JOIN Supplier s ON po.SupplierID = s.SupplierID;


-- 6
SELECT 
    sh.ShipmentID,
    w.ContactInfo AS WarehouseContact,
    e.Name AS ManagerName,
    p.Name AS ProductName,
    sd.Quantity AS QuantityShipped,
    sh.TrackingNumber
FROM Shipment sh
JOIN Warehouse w ON sh.WarehouseID = w.WarehouseID
JOIN Employee e ON w.ManagerID = e.EmployeeID
JOIN ShipmentDetail sd ON sh.ShipmentID = sd.ShipmentID
JOIN Product p ON sd.ProductID = p.ProductID;

-- 7
SELECT 
    c.CustomerID,
    c.Name AS CustomerName,
    so.OrderID,
    SUM(sod.Quantity * sod.UnitPrice) AS OrderValue,
    RANK() OVER (PARTITION BY c.CustomerID ORDER BY SUM(sod.Quantity * sod.UnitPrice) DESC) AS OrderRank
FROM Customer c
JOIN SalesOrder so ON c.CustomerID = so.CustomerID
JOIN SalesOrderDetail sod ON so.OrderID = sod.OrderID
GROUP BY c.CustomerID, c.Name, so.OrderID
ORDER BY c.CustomerID, OrderRank;


-- 8
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    so.OrderID,
    so.OrderDate,
    sod.Quantity,
    LAG(sod.Quantity) OVER (PARTITION BY p.ProductID ORDER BY so.OrderDate) AS PrevQuantity,
    LEAD(sod.Quantity) OVER (PARTITION BY p.ProductID ORDER BY so.OrderDate) AS NextQuantity
FROM Product p
JOIN SalesOrderDetail sod ON p.ProductID = sod.ProductID
JOIN SalesOrder so ON sod.OrderID = so.OrderID
ORDER BY p.ProductID, so.OrderDate;


-- 9
SELECT 
    c.CustomerID,
    c.Name AS CustomerName,
    COUNT(so.OrderID) AS TotalOrders,
    SUM(sod.Quantity * sod.UnitPrice) AS TotalAmountSpent,
    MAX(so.OrderDate) AS LastOrderDate
FROM Customer c
JOIN SalesOrder so ON c.CustomerID = so.CustomerID
JOIN SalesOrderDetail sod ON so.OrderID = sod.OrderID
GROUP BY c.CustomerID, c.Name
ORDER BY TotalAmountSpent DESC;


-- 10

SELECT 
    s.SupplierID,
    s.Name AS SupplierName,
    SUM(pod.Quantity * pod.UnitPrice) AS TotalSalesAmount
FROM Supplier s
JOIN PurchaseOrder po ON s.SupplierID = po.SupplierID
JOIN PurchaseOrderDetail pod ON po.OrderID = pod.OrderID
JOIN Product p ON pod.ProductID = p.ProductID
WHERE s.SupplierID = 3    
GROUP BY s.SupplierID, s.Name;




















