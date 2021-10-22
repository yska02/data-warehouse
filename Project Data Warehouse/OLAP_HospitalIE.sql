CREATE DATABASE OLAP_HospitalIE
GO
USE OLAP_HospitalIE

use master
drop database OLAP_HospitalIE

-- DIMENSION

CREATE TABLE TimeDimension(
	TimeCode INT PRIMARY KEY IDENTITY,
	[Date] DATE,
	[Month] INT,
	[Quarter] INT,
	[Year] INT
)

CREATE TABLE MedicineDimension(
	MedicineCode INT PRIMARY KEY IDENTITY,
	MedicineID INT,
	MedicineName VARCHAR(255),
	MedicineExpiredDate DATE,
	MedicineBuyingPrice BIGINT,
	MedicineSellingPrice BIGINT
)

CREATE TABLE DoctorDimension(
	DoctorCode INT PRIMARY KEY IDENTITY,
	DoctorID INT,
	DoctorName VARCHAR(255),
	DoctorDOB DATE,
	DoctorSalary BIGINT, -- Historical
	DoctorAddress VARCHAR(255),-- Changed
	ValidFrom DATETIME,
	ValidTo DATETIME
)

CREATE TABLE StaffDimension(
	StaffCode INT PRIMARY KEY IDENTITY,
	StaffID INT,
	StaffName VARCHAR(255),
	StaffDOB DATE,
	StaffSalary BIGINT,-- Historical
	StaffAddress VARCHAR(255),-- Changed
	ValidFrom DATETIME,
	ValidTo DATETIME
)

CREATE TABLE CustomerDimension(
	CustomerCode INT PRIMARY KEY IDENTITY,
	CustomerID INT,
	CustomerName VARCHAR(255),
	CustomerAddress VARCHAR(255),-- Changed
	CustomerGender CHAR(1)-- Derived: F for Female, M for Male
)

CREATE TABLE BenefitDimension(
	BenefitCode INT PRIMARY KEY IDENTITY,
	BenefitID INT,
	BenefitName VARCHAR(255),
	BenefitPrice BIGINT,-- Historical
	ValidFrom DATETIME,
	ValidTo DATETIME
)

CREATE TABLE TreatmentDimension(
	TreatmentCode INT PRIMARY KEY IDENTITY,
	TreatmentID INT,
	TreatmentName VARCHAR(255),
	TreatmentPrice BIGINT,-- Historical
	ValidFrom DATETIME,
	ValidTo DATETIME
)

CREATE TABLE DistributorDimension(
	DistributorCode INT PRIMARY KEY IDENTITY,
	DistributorID INT,
	DistributorName VARCHAR(255),
	DistributorAddress VARCHAR(255),-- Changed
	DistributorCityName VARCHAR(255),
	DistributorPhone VARCHAR(255)-- Changed
)

-- FACT

CREATE TABLE SalesTrFact(
	TimeCode INT,
	MedicineCode INT,
	StaffCode INT,
	CustomerCode INT,
	[TotalSalesEarning] BIGINT, 
	[TotalMedicineSold] BIGINT
)

CREATE TABLE PurchaseTrFact(
	TimeCode INT,
	MedicineCode INT,
	StaffCode INT,
	DistributorCode INT,
	[TotalPurchaseCost] BIGINT,
	[TotalMedicinePurchased] BIGINT
)

CREATE TABLE SubscriptionTrFact(
	TimeCode INT,
	CustomerCode INT,
	StaffCode INT,
	BenefitCode INT,
	[TotalSubscriptionEarning] BIGINT,
	[SubscriberCount] BIGINT
)

CREATE TABLE ServiceTrFact(
	TimeCode INT,
	CustomerCode INT,
	TreatmentCode INT,
	DoctorCode INT,
	[TotalServiceEarning] BIGINT,
	[NumberOfDoctor] BIGINT
)

CREATE TABLE FilterTimeStamp(
	TableName VARCHAR(255) PRIMARY KEY,
	LastETL DATETIME
)

-- CustomerDimension
SELECT CustomerID, CustomerName, CustomerAddress, CustomerGender
FROM OLTP_HospitalIE..MsCustomer
-- CustomerGender == 'Male"?"M":"F"
--
SELECT * FROM CustomerDimension

-- MedicineDimension
SELECT MedicineID, MedicineName, MedicineExpiredDate, MedicineBuyingPrice, MedicineSellingPrice
FROM OLTP_HospitalIE..MsMedicine
--
SELECT * FROM MedicineDimension

-- DoctorDimension
SELECT DoctorID, DoctorName, DoctorDOB, DoctorSalary, DoctorAddress
FROM OLTP_HospitalIE..MsDoctor
--
SELECT * FROM DoctorDimension

-- StaffDimension
SELECT StaffID, StaffName, StaffDOB, StaffSalary, StaffAddress
FROM OLTP_HospitalIE..MsStaff
--
SELECT * FROM StaffDimension

-- BenefitDimension
SELECT BenefitID, BenefitName, BenefitPrice
FROM OLTP_HospitalIE..MsBenefit
--
SELECT * FROM BenefitDimension

-- TreatmentDimension
SELECT TreatmentID, TreatmentName, TreatmentPrice
FROM OLTP_HospitalIE..MsTreatment
--
SELECT * FROM TreatmentDimension
 
 -- DistributorDimension
SELECT DistributorID, DistributorName, DistributorAddress, CityName, DistributorPhone
FROM OLTP_HospitalIE..MsDistributor d
JOIN OLTP_HospitalIE..MsCity c
ON c.CityID = d.CityID
 --
SELECT * FROM DistributorDimension

-- TimeDimension
IF NOT EXISTS ( SELECT *  FROM FilterTimeStamp WHERE TableName = 'TimeDimension')
BEGIN
SELECT
	[Date] = CAST(dates.[DATE] AS DATE),
	[Month] = MONTH(dates.[DATE]),
	[Quarter] = DATEPART(QUARTER, dates.[DATE]),
	[Year] = YEAR(dates.[DATE])
FROM (
	SELECT [DATE] = PurchaseDate
	FROM OLTP_HospitalIE..TrPurchaseHeader
	UNION
	SELECT [DATE] = SubscriptionStartDate
	FROM OLTP_HospitalIE..TrSubscriptionHeader
	UNION
	SELECT [DATE] = SalesDate
	FROM OLTP_HospitalIE..TrSalesHeader
) AS dates
END
ELSE
BEGIN
SELECT
	[Date] = CAST(dates.[DATE] AS DATE),
	[Month] = MONTH(dates.[DATE]),
	[Quarter] = DATEPART(QUARTER, dates.[DATE]),
	[Year] = YEAR(dates.[DATE])
FROM (
	SELECT [DATE] = PurchaseDate
	FROM OLTP_HospitalIE..TrPurchaseHeader
	UNION
	SELECT [DATE] = SubscriptionStartDate
	FROM OLTP_HospitalIE..TrSubscriptionHeader
	UNION
	SELECT [DATE] = SalesDate
	FROM OLTP_HospitalIE..TrSalesHeader
) AS dates
WHERE dates.[DATE] > ( SELECT LastETL  FROM FilterTimeStamp WHERE TableName = 'TimeDimension' )
END
--
SELECT * FROM FilterTimeStamp

-- FilterTimeStamp TimeDimension
IF EXISTS( SELECT *  FROM FilterTimeStamp WHERE TableName = 'TimeDimension')
BEGIN
	UPDATE FilterTimeStamp SET LastETL = GETDATE() 
	WHERE TableName = 'TimeDimension' 
END
ELSE
BEGIN
	INSERT INTO FilterTimeStamp VALUES('TimeDimension', GETDATE())
END
--
SELECT * FROM TimeDimension

-- SalesTrFact
IF NOT EXISTS ( SELECT *  FROM FilterTimeStamp WHERE TableName = 'SalesTrFact')
BEGIN
	SELECT 
		TimeCode, 
		MedicineCode, 
		StaffCode, 
		CustomerCode,
		[TotalSalesEarning] = SUM(Quantity * MedicineSellingPrice),
		[TotalMedicineSold] = SUM(Quantity)
	FROM 
		OLTP_HospitalIE..TrSalesHeader sh
		JOIN OLTP_HospitalIE..TrSalesDetail sd
		ON sh.SalesID = sd.SalesID
		JOIN TimeDimension t
		ON sh.SalesDate = t.Date
		JOIN StaffDimension s
		ON sh.StaffID = s.StaffID
		JOIN CustomerDimension c
		ON sh.CustomerID = c.CustomerID
		LEFT JOIN MedicineDimension m
		ON sd.MedicineID = m.MedicineID
	GROUP BY
		TimeCode, 
		MedicineCode, 
		StaffCode, 
		CustomerCode
END
ELSE
BEGIN
	SELECT
		TimeCode, 
		MedicineCode, 
		StaffCode, 
		CustomerCode,
		[TotalSalesEarning] = SUM(Quantity * MedicineSellingPrice),
		[TotalMedicineSold] = SUM(Quantity)
	FROM 
		TimeDimension t,
		MedicineDimension m,
		StaffDimension s,
		CustomerDimension c,
		OLTP_HospitalIE..TrSalesHeader sh,
		OLTP_HospitalIE..TrSalesDetail sd
	WHERE 
		sh.SalesID = sd.SalesID AND
		sh.SalesDate = t.Date AND 
		sh.StaffID = s.StaffID AND
		sh.CustomerID = c.CustomerID AND
		sd.MedicineID = m.MedicineID AND
		t.[DATE] > ( SELECT LastETL  FROM FilterTimeStamp WHERE TableName = 'SalesTrFact' )
	GROUP BY
		TimeCode, 
		MedicineCode, 
		StaffCode, 
		CustomerCode
END

-- FilterTimeStamp SalesTrFact
IF EXISTS( SELECT *  FROM FilterTimeStamp WHERE TableName = 'SalesTrFact')
BEGIN
	UPDATE FilterTimeStamp SET LastETL = GETDATE() 
	WHERE TableName = 'SalesTrFact' 
END
ELSE
BEGIN
	INSERT INTO FilterTimeStamp VALUES('SalesTrFact', GETDATE())
END

-- PurchaseTrFact
IF NOT EXISTS ( SELECT *  FROM FilterTimeStamp WHERE TableName = 'PurchaseTrFact')
BEGIN
	SELECT 
		TimeCode,
		MedicineCode,
		StaffCode,
		DistributorCode,
		[TotalPurchaseCost] = SUM(Quantity * MedicineBuyingPrice),
		[TotalMedicinePurchased] = SUM(Quantity)
	FROM 
		TimeDimension t,
		MedicineDimension m,
		StaffDimension s,
		DistributorDimension dis,
		OLTP_HospitalIE..TrPurchaseHeader ph,
		OLTP_HospitalIE..TrPurchaseDetail pd
	WHERE 
		CAST(PurchaseDate AS DATE) = t.Date AND 
		ph.StaffID = s.StaffID AND
		ph.DistributorID = dis.DistributorID AND
		pd.MedicineID = m.MedicineID
	GROUP BY
		TimeCode, 
		MedicineCode, 
		StaffCode, 
		DistributorCode
END
ELSE
BEGIN
	SELECT 
		TimeCode,
		MedicineCode,
		StaffCode,
		DistributorCode,
		[TotalPurchaseCost] = SUM(Quantity * MedicineBuyingPrice),
		[TotalMedicinePurchased] = SUM(Quantity)
	FROM 
		TimeDimension t,
		MedicineDimension m,
		StaffDimension s,
		DistributorDimension dis,
		OLTP_HospitalIE..TrPurchaseHeader ph,
		OLTP_HospitalIE..TrPurchaseDetail pd
	WHERE
		ph.PurchaseID = pd.PurchaseID AND
		CAST(PurchaseDate AS DATE) = t.Date AND 
		ph.StaffID = s.StaffID AND
		ph.DistributorID = dis.DistributorID AND
		pd.MedicineID = m.MedicineID AND
		t.[DATE] > ( SELECT LastETL  FROM FilterTimeStamp WHERE TableName = 'PurchaseTrFact' )
	GROUP BY
		TimeCode, 
		MedicineCode, 
		StaffCode, 
		DistributorCode
END

-- FilterTimeStamp PurchaseTrFact
IF EXISTS( SELECT *  FROM FilterTimeStamp WHERE TableName = 'PurchaseTrFact')
BEGIN
	UPDATE FilterTimeStamp SET LastETL = GETDATE() 
	WHERE TableName = 'PurchaseTrFact' 
END
ELSE
BEGIN
	INSERT INTO FilterTimeStamp VALUES('PurchaseTrFact', GETDATE())
END

-- SubscriptionTrFact
IF NOT EXISTS ( SELECT *  FROM FilterTimeStamp WHERE TableName = 'SubscriptionTrFact')
BEGIN
	SELECT 
		TimeCode,
		CustomerCode,
		StaffCode,
		BenefitCode,
		[TotalSubscriptionEarning] = SUM(BenefitPrice),
		[SubscriberCount] = COUNT(sh.CustomerID)
	FROM 
		TimeDimension t,
		CustomerDimension c,
		StaffDimension s,
		BenefitDimension b,
		OLTP_HospitalIE..TrSubscriptionHeader sh,
		OLTP_HospitalIE..TrSubscriptionDetail sd
	WHERE 
		sh.SubscriptionID = sd.SubscriptionID AND
		CAST(SubscriptionStartDate AS DATE) = t.Date AND 
		sh.StaffID = s.StaffID AND
		sh.CustomerID = c.CustomerID AND
		sd.BenefitID= b.BenefitID
	GROUP BY
		TimeCode, 
		CustomerCode, 
		StaffCode, 
		BenefitCode
END
ELSE
BEGIN
	SELECT 
		TimeCode,
		CustomerCode,
		StaffCode,
		BenefitCode,
		[TotalSubscriptionEarning] = SUM(BenefitPrice),
		[SubscriberCount] = COUNT(sh.CustomerID)
	FROM 
		TimeDimension t,
		CustomerDimension c,
		StaffDimension s,
		BenefitDimension b,
		OLTP_HospitalIE..TrSubscriptionHeader sh,
		OLTP_HospitalIE..TrSubscriptionDetail sd
	WHERE 
		sh.SubscriptionID = sd.SubscriptionID AND
		CAST(SubscriptionStartDate AS DATE) = t.Date AND 
		sh.StaffID = s.StaffID AND
		sh.CustomerID = c.CustomerID AND
		sd.BenefitID= b.BenefitID AND
		t.[DATE] > ( SELECT LastETL  FROM FilterTimeStamp WHERE TableName = 'SubscriptionTrFact' )
	GROUP BY
		TimeCode, 
		CustomerCode, 
		StaffCode, 
		BenefitCode
END

-- FilterTimeStamp SubscriptionTrFact
IF EXISTS( SELECT *  FROM FilterTimeStamp WHERE TableName = 'SubscriptionTrFact')
BEGIN
	UPDATE FilterTimeStamp SET LastETL = GETDATE() 
	WHERE TableName = 'SubscriptionTrFact' 
END
ELSE
BEGIN
	INSERT INTO FilterTimeStamp VALUES('SubscriptionTrFact', GETDATE())
END


-- ServiceTrFact
IF NOT EXISTS ( SELECT *  FROM FilterTimeStamp WHERE TableName = 'ServiceTrFact')
BEGIN
	SELECT 
		TimeCode,
		CustomerCode,
		TreatmentCode,
		DoctorCode,
		[TotalServiceEarning] = SUM(Quantity * TreatmentPrice),
		[NumberOfDoctor] = COUNT(sh.DoctorID)
	FROM 
		TimeDimension t,
		DoctorDimension d,
		TreatmentDimension tm,
		CustomerDimension c,
		OLTP_HospitalIE..TrServiceHeader sh,
		OLTP_HospitalIE..TrServiceDetail sd
	WHERE 
		sh.ServiceID = sd.ServiceID AND
		CAST(ServiceDate AS DATE) = t.Date AND 
		sh.DoctorID = d.DoctorID AND
		sh.CustomerID = c.CustomerID AND
		sd.TreatmentID = tm.TreatmentID 
	GROUP BY
		TimeCode, 
		CustomerCode, 
		DoctorCode, 
		TreatmentCode
END
ELSE
BEGIN
	SELECT 
		TimeCode,
		CustomerCode,
		TreatmentCode,
		DoctorCode,
		[TotalServiceEarning] = SUM(Quantity * TreatmentPrice),
		[NumberOfDoctor] = COUNT(sh.DoctorID)
	FROM 
		TimeDimension t,
		DoctorDimension d,
		TreatmentDimension tm,
		CustomerDimension c,
		OLTP_HospitalIE..TrServiceHeader sh,
		OLTP_HospitalIE..TrServiceDetail sd
	WHERE 
		sh.ServiceID = sd.ServiceID AND
		CAST(ServiceDate AS DATE) = t.Date AND 
		sh.DoctorID = d.DoctorID AND
		sh.CustomerID = c.CustomerID AND
		sd.TreatmentID = tm.TreatmentID AND
		t.[DATE] > ( SELECT LastETL  FROM FilterTimeStamp WHERE TableName = 'ServiceTrFact' )
	GROUP BY
		TimeCode, 
		CustomerCode, 
		DoctorCode, 
		TreatmentCode
END




-- FilterTimeStamp ServiceTrFact
IF EXISTS( SELECT *  FROM FilterTimeStamp WHERE TableName = 'ServiceTrFact')
BEGIN
	UPDATE FilterTimeStamp SET LastETL = GETDATE() 
	WHERE TableName = 'ServiceTrFact' 
END
ELSE
BEGIN
	INSERT INTO FilterTimeStamp VALUES('ServiceTrFact', GETDATE())
END

