create database if not exists banking;
use banking;
Create table DC_banking
(
Customer_ID varchar(255),
Customer_Name varchar(255),
Account_Number bigint,
Transaction_Date date,
Transaction_Type varchar(255),
Amount double,
Balance double,
Descriptive varchar(255),
Branch varchar(255),
Transaction_Method varchar(255),
Currency varchar(255),
Bank_Name varchar(255)
);

select * from DC_banking;
set global LOCAL_INFILE=ON;
LOAD DATA LOCAL INFILE 'D:/Excelr/Bank Analytics Project/SQL/Debit and Credit banking_data.csv' INTO TABLE dc_banking
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

select * from dc_banking;
ALTER TABLE dc_banking
DROP COLUMN Descriptive;

ALTER TABLE dc_banking
MODIFY COLUMN Amount DECIMAL(10,2),
MODIFY COLUMN Balance DECIMAL(10,2),
MODIFY COLUMN Transaction_Type ENUM('Credit', 'Debit');

select * from dc_banking;
ALTER TABLE dc_banking
DROP COLUMN Currency;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Total Credit Amount
SELECT 
	concat(ROUND(SUM(Amount) / 1000000, 2), 'M') 
    AS Total_Credit_Amount
FROM dc_banking
WHERE Transaction_Type = 'Credit';

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Total Debit Amount
SELECT 
    concat(Round(SUM(Amount) / 1000000, 2), 'M') 
    AS Total_Debit_Amount
FROM dc_banking
WHERE Transaction_Type = 'Debit';

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Credit to Debit Ratio
SELECT 
    SUM(CASE WHEN `Transaction_Type` = 'Credit' THEN Amount ELSE 0 END) /
    NULLIF(SUM(CASE WHEN `Transaction_Type` = 'Debit' THEN Amount ELSE 0 END), 0) 
    AS Credit_Debit_Ratio
FROM dc_banking;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Net Transaction Amount
SELECT 
    CONCAT(ROUND((
        SUM(CASE WHEN `Transaction_Type` = 'Credit' THEN Amount ELSE 0 END) -
        SUM(CASE WHEN `Transaction_Type` = 'Debit' THEN Amount ELSE 0 END)
    ) / 1000000, 2), 'M') AS Net_Transaction_Amount
FROM dc_banking;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Account Acticity Ratio by Branch
SELECT 
    Branch,
    ROUND(COUNT(*) / MAX(Balance), 4) AS Account_Activity_Ratio
FROM dc_banking
GROUP BY Branch;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Transactions per Day/Week/Month
-- Daily
SELECT `Transaction_Date`, COUNT(*) AS Daily_Transactions
FROM dc_banking
GROUP BY `Transaction_Date`;
---------------------------------------------------------------
-- Weekly
SELECT 
    YEAR(Transaction_Date) AS Year,
    WEEK(Transaction_Date, 1) AS Week_Number,
    COUNT(*) AS Transaction_Count
FROM dc_banking
GROUP BY YEAR(Transaction_Date), WEEK(Transaction_Date, 1)
ORDER BY Year, Week_Number;
---------------------------------------------------------------
-- Monthly
SELECT 
    MONTHNAME(`Transaction_Date`) AS Month,
    COUNT(*) AS Monthly_Transactions
FROM dc_banking
GROUP BY MONTH(`Transaction_Date`), MONTHNAME(`Transaction_Date`)
ORDER BY MONTH(`Transaction_Date`);

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Total Transaction Amount by Branch
SELECT Branch,
    CONCAT(ROUND(SUM(Amount) / 1000000, 2), 'M') AS Transaction_Amount
FROM dc_banking
GROUP BY Branch;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Transaction Volume by Bank
SELECT `Bank_Name`, count(Amount) AS Transaction_Volume
FROM dc_banking
GROUP BY `Bank_Name`;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Transaction Method Distribution
SELECT 
    `Transaction_Method`, 
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dc_banking), 2) AS Transaction_Percentage
FROM dc_banking
GROUP BY `Transaction_Method`;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Branch Transaction Growth
WITH MonthlySums AS (
    SELECT 
        Branch,
        DATE_FORMAT(Transaction_Date, '%b') AS MonthShort,
        MONTH(Transaction_Date) AS MonthNum,
        SUM(Amount) AS TotalAmount
    FROM dc_banking
    GROUP BY Branch, MonthNum, MonthShort
),
WithGrowth AS (
    SELECT 
        Branch,
        MonthShort,
        MonthNum,
        TotalAmount,
        LAG(TotalAmount) OVER (PARTITION BY Branch ORDER BY MonthNum) AS PrevMonthAmount
    FROM MonthlySums
),
GrowthCalc AS (
    SELECT 
        Branch,
        MonthShort,
        MonthNum,
        ROUND((TotalAmount - PrevMonthAmount) / NULLIF(PrevMonthAmount, 0) * 100, 2) AS GrowthPercent
    FROM WithGrowth
)
SELECT 
    MonthShort AS `Month`,
    MAX(CASE WHEN Branch = 'City Center Branch' THEN CONCAT(GrowthPercent, '%') END) AS `City Center Branch`,
    MAX(CASE WHEN Branch = 'Downtown Branch' THEN CONCAT(GrowthPercent, '%') END) AS `Downtown Branch`,
    MAX(CASE WHEN Branch = 'East Branch' THEN CONCAT(GrowthPercent, '%') END) AS `East Branch`,
    MAX(CASE WHEN Branch = 'Main Branch' THEN CONCAT(GrowthPercent, '%') END) AS `Main Branch`,
    MAX(CASE WHEN Branch = 'North Branch' THEN CONCAT(GrowthPercent, '%') END) AS `North Branch`,
    MAX(CASE WHEN Branch = 'Suburban Branch' THEN CONCAT(GrowthPercent, '%') END) AS `Suburban Branch`
FROM GrowthCalc
GROUP BY MonthNum, MonthShort
ORDER BY MonthNum;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- High-Risk Transaction Flag
SELECT 
    Bank_Name,
    COUNT(*) AS High_Risk_Transaction_Count
FROM dc_banking
WHERE Amount > 3500
GROUP BY Bank_Name;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Suspicious Transaction Frequency
SELECT 
    Bank_Name,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 1 AND Amount > 3500 THEN 1 END) AS January,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 2 AND Amount > 3500 THEN 1 END) AS February,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 3 AND Amount > 3500 THEN 1 END) AS March,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 4 AND Amount > 3500 THEN 1 END) AS April,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 5 AND Amount > 3500 THEN 1 END) AS May,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 6 AND Amount > 3500 THEN 1 END) AS June,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 7 AND Amount > 3500 THEN 1 END) AS July,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 8 AND Amount > 3500 THEN 1 END) AS August,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 9 AND Amount > 3500 THEN 1 END) AS September,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 10 AND Amount > 3500 THEN 1 END) AS October,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 11 AND Amount > 3500 THEN 1 END) AS November,
    COUNT(CASE WHEN MONTH(Transaction_Date) = 12 AND Amount > 3500 THEN 1 END) AS December
FROM 
    dc_banking
WHERE 
    YEAR(Transaction_Date) = 2024
GROUP BY 
    Bank_Name;