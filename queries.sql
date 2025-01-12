// Below is the staging table structure where all the source files data will be loaded

CREATE OR REPLACE TABLE Stage_Table (
Customer_Name VARCHAR(255) NOT NULL,
Customer_ID VARCHAR(18) NOT NULL,
Customer_Open_Date DATE NOT NULL,
Last_Consulted_Date DATE,
Vaccination_ID CHAR(5),
Doctor_Name VARCHAR(255),
State CHAR(5),
Country CHAR(5),
PostAL_Code INT,
DOB DATE,
Act_Cust CHAR(1) CHECK (Act_Cust IN ('A', 'I')), -- 'A' for active, 'I' for inactive
Record_Type CHAR(1) NOT NULL CHECK (Record_Type IN ('H', 'D')) -- Header or Detail
);

//Below procedure will create all country tables  with the use of staging table country column and also loading the data into each country table
  
CREATE OR REPLACE PROCEDURE load_data_dynamically()
RETURNS varchar(1000)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
    var result = snowflake.execute(`SELECT DISTINCT Country FROM stage_table`);
    while (result.next()) {
        var country = result.getColumnValue(1);
        var createTable = `
            CREATE OR REPLACE TABLE country_${country} (
                Customer_Name VARCHAR(255),
                Customer_ID VARCHAR(18),
                Open_Date DATE,
                Last_Consulted_Date DATE,
                Vaccination_ID CHAR(5),
                Doctor_Name VARCHAR(255),
                State CHAR(5),
                Country CHAR(5),
                Post_Code INT,
                DOB DATE,
                Act_Cust CHAR(1),
                Age INT,
                Days_Since_Last_Consulted varchar(100)
            )`;
        snowflake.execute(createTable);
        
        var loadData = `
            MERGE INTO country_${country} AS target
            USING (
                SELECT *,
                       DATEDIFF(YEAR, DOB, CURRENT_DATE) AS Age,
                       CASE 
                        WHEN DATEDIFF('day', Last_Consulted_Date, CURRENT_DATE()) > 30 THEN 'YES'
                        ELSE 'NO'
                    END AS Days_Since_Last_Consulted
                FROM validated_table
                WHERE Country = '${country}'
            ) AS source
            ON target.Customer_ID = source.Customer_ID
            WHEN MATCHED THEN
                UPDATE SET
                    Last_Consulted_Date = source.Last_Consulted_Date
            WHEN NOT MATCHED THEN
                INSERT (
                    Customer_Name, Customer_ID, Open_Date, Last_Consulted_Date,
                    Vaccination_ID, Doctor_Name, State, Country, Post_Code,
                    DOB, Act_Cust, Age, Days_Since_Last_Consulted
                )
                VALUES (
                    source.Customer_Name, source.Customer_ID, source.Open_Date, source.Last_Consulted_Date,
                    source.Vaccination_ID, source.Doctor_Name, source.State, source.Country, source.Post_Code,
                    source.DOB, source.Act_Cust, source.Age, source.Days_Since_Last_Consulted
                )`;
        snowflake.execute(loadData);
    }
    return 'Country tables creation and loading completed...';

$$;
