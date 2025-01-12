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
Act_Cust CHAR(1) CHECK (Is_Active IN ('A', 'I')), -- 'A' for active, 'I' for inactive
Record_Type CHAR(1) NOT NULL CHECK (Record_Type IN ('H', 'D')) -- Header or Detail
);

//Below procedure will create all country tables with the use of staging table country column
  
CREATE OR REPLACE PROCEDURE create_country_tables()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
{
    var country_list = [];
    
    // Step 1: Fetch the unique countries from the Stage_Table
    var result = snowflake.createStatement({
        sqlText: `
            SELECT DISTINCT Country 
            FROM Stage_Table
            WHERE Country IS NOT NULL
        `
    }).execute();

    while (result.next()) {
        country_list.push(result.getColumnValue(1));
    }
    
    // Step 2: Loop through each country and create a country-specific table
    country_list.forEach((country) => {
        var table_name = `Table_${country}`;
        
        var create_table_query = `
            CREATE OR REPLACE TABLE ${table_name} (
                Customer_Name VARCHAR(255) NOT NULL,
                Customer_ID VARCHAR(18) NOT NULL PRIMARY KEY,
                Open_Date DATE NOT NULL,
                Last_Consulted_Date DATE,
                Vaccination_ID CHAR(5),
                Doctor_Name VARCHAR(255),
                State CHAR(5),
                Postal_Code INT,
                DOB DATE,
                Act_Cust CHAR(1) CHECK (Is_Active IN ('A', 'I')),
                Age INT AS (DATEDIFF('year', DOB, CURRENT_DATE())), -- Derived column: Age
                Days_Since_Last_Consulted_Over_30 STRING AS 
                    CASE 
                        WHEN DATEDIFF('day', Last_Consulted_Date, CURRENT_DATE()) > 30 THEN 'YES'
                        ELSE 'NO'
                    END -- Derived column: Days since last consulted > 30
            );
        `;
        
        // Execute the create table query
        snowflake.createStatement({
            sqlText: create_table_query
        }).execute();
    });

   
    return `Country-specific tables are created successfully for ${country_list.length} countries: ${country_list.join(', ')}`;
}
$$;


