//create a table where the valid data will be inserted

CREATE OR REPLACE TABLE validated_table (
Customer_Name VARCHAR,
Customer_ID VARCHAR,
Open_Date DATE,
Last_Consulted_Date DATE,
Vaccination_ID VARCHAR,
Doctor_Name VARCHAR,
State VARCHAR,
Country VARCHAR,
Post_Code INT,
DOB DATE,
Is_Active CHAR(1),
Age INT,
Days_Since_Last_Consulted varchar(100)
);

// load the data into staging table from the source files assuming here the source as azure blob

COPY INTO stage_table
FROM 'azure://<container>/<path>/'
CREDENTIALS=(AZURE_SAS_TOKEN='<your_sas_token>')
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


//create stream on the staging table to track CDC

CREATE OR REPLACE STREAM staging_stream ON TABLE stage_table;

//create a task to load the data from the stream to validate table

CREATE OR REPLACE TASK validate_and_transform_task
WAREHOUSE = compute_wh
SCHEDULE = '1 MINUTE'
AS
INSERT INTO validated_table
SELECT 
Customer_Name,
Customer_ID,
Open_Date,
Last_Consulted_Date,
Vaccination_ID,
Doctor_Name,
State,
Country,
Post_Code,
DOB,
Is_Active,
DATEDIFF('YEAR', DOB, CURRENT_DATE) AS Age,
DATEDIFF('DAY', Last_Consu  lted_Date, CURRENT_DATE) AS Days_Since_Last_Consulted
FROM staging_stream
WHERE Is_Active = 'A';

//Country specific stream creation
CREATE OR REPLACE STREAM validated_stream ON TABLE validated_table;


//below procedure will create the tasks dynamically and insert each countries data into respective tables
CREATE OR REPLACE PROCEDURE create_country_specific_tasks()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
// Create a statement to fetch distinct countries from validated_table
var sql_command = "SELECT DISTINCT Country FROM validated_table";

// Execute the SQL query to get distinct countries
var stmt1 = snowflake.createStatement({sqlText: sql_command});
var result_set = stmt1.execute();

// Loop through each country in the result set
while (result_set.next()) {
var country_name = result_set.getColumnValue(1);  // Get country name

// Dynamically create task name and SQL query
var task_name = 'task_partition_' + country_name;
var sql_query = `
CREATE OR REPLACE TASK ` + task_name + `
WAREHOUSE = compute_wh
SCHEDULE = '1 HOUR'
AS
INSERT INTO country_table_` + country_name + `
SELECT * FROM validated_stream
WHERE Country = '` + country_name + `';
`;

// Create a statement to create the task dynamically
var stmt2 = snowflake.createStatement({sqlText: sql_query});
stmt2.execute();  // Execute the task creation query
}

// Return success message
return 'Tasks created successfully for each country.';
} catch(err) {
// Catch any errors and return the error message
return "Failed to create tasks: " + err.message;
}
$$;




