import csv
import random
from datetime import datetime, timedelta

# Function to generate random dates
def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

# Function to generate random customer data
def generate_customer_data(num_records):
    names = ["Alex", "John", "Mathew", "Matt", "Jacob", "Emily", "Sophia", "Olivia", "Liam", "Noah"]
    states = ["SA", "TN", "WAS", "BOS", "VIC", "CA", "TX", "FL", "NY", "NJ"]
    countries = ["USA", "IND", "PHIL", "NYC", "AU", "CAN", "UK", "GER", "FRA", "JPN"]
    vaccination_ids = ["MVD", "FLU", "COV", "HPT", "MMR"]
    dr_names = ["Paul", "John", "Smith", "Doe", "Brown"]

    start_date = datetime.strptime('20100101', '%Y%m%d')
    end_date = datetime.strptime('20201231', '%Y%m%d')

    records = []
    for i in range(num_records):
        name = random.choice(names)
        customer_id = random.randint(1000, 999999)
        open_date = random_date(start_date, end_date).strftime('%Y%m%d')
        last_consulted_date = random_date(start_date, end_date).strftime('%Y%m%d')
        vaccination_id = random.choice(vaccination_ids)
        dr_name = random.choice(dr_names)
        state = random.choice(states)
        country = random.choice(countries)
        dob = random_date(datetime.strptime('19500101', '%Y%m%d'), datetime.strptime('20001231', '%Y%m%d')).strftime('%m%d%Y')
        is_active = random.choice(['A', 'I'])

        records.append([name, customer_id, open_date, last_consulted_date, vaccination_id, dr_name, state, country, dob, is_active])

    return records

# Generate 250 records
num_records = 250
customer_data = generate_customer_data(num_records)

# Write to CSV file
with open(r'C:\Users\panka\OneDrive\Desktop\customer_data.csv','w',newline='') as file:
    writer = csv.writer(file, delimiter='|')
    writer.writerow(['H', 'Customer_Name', 'Customer_Id', 'Open_Date', 'Last_Consulted_Date', 'Vaccination_Id', 'Dr_Name', 'State', 'Country', 'DOB', 'Is_Active'])
    for record in customer_data:
        writer.writerow(['D'] + record)
