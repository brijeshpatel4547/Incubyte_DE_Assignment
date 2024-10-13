# Incubyte_DE_Assignment
**Current Scenario:**
We maintain all customers in one database. There are heaps of customers with user cards to my hospital. So, I decided to split up the customers based on the country and load them into corresponding country tables.
To pull the customers as per Country, my developers should know what are all the places the Customer Data is available. So, the data extracting will be done by our Source System. They will pull the all the relevant customer data and will give us a Data file.

Table Structure:

![image](https://github.com/user-attachments/assets/f0dcfa79-d3fa-4777-94b7-00d276730951)

**Tools and Technologies Used:**
1. PySpark
2. Datbricks Community Edition
3. PostgreSQL

**Solution Deliverables:**
1. Create table queries
2. Create the above tables with additional derived columns: age and days since last consulted >30
3. Create necessary validations.
4. If we move forward with an interview we would like to see the demonstration


I've have used PySpark to solve these problem as there are billions of records in assumption, it is better to use PySpark for perform transformations and cleaning due to it's parallel processing, rather than 
traditional python, this will improve the performance of our ETL Job.

**Generating Data:** Used Python Code to generate random 250 records of similar format(Record Generator.py).
**Creating Database:** Uploaded the data into database created using postgreSQL.
**Performing Transformation & Cleaning:** Using PySpark code, corrected the datatypes, removed unwanted columns and added few additional columns as per the request.
**Partitioning and Exporting:** At last the data is partitioned based on the country and all the files are stored in different folders.
