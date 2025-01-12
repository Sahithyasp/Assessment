// validation to check all the mandatory fields are not blank
SELECT *
FROM Stage_Table
WHERE Customer_Name IS NULL 
OR Customer_ID IS NULL 
OR Open_Date IS NULL;

// validation to check duplicate customerids from the staging table

SELECT Customer_ID, COUNT(*)
FROM Stage_Table
WHERE Record_Type = 'D'
GROUP BY Customer_ID
HAVING COUNT(*) > 1;

// validation to check only the active customers are loaded in the table

SELECT * 
FROM Stage_Table
WHERE Act_Cust = 'A';

//validation to check the date formats are same in the staging table while loading the data

SELECT * 
FROM Stage_Table
WHERE TRY_TO_DATE(Open_Date, 'YYYYMMDD') IS NULL;







