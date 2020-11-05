'''
Python script to create fake database of users and purchase orders for use in ETL pipeline
'''
from faker import Faker
import numpy as np
import random
import json
from faker.providers import BaseProvider
from datetime import datetime
import pandas as pd

# ---- function defintions -------


def create_json_file(filename: str, dict):
    '''function used to dump json file from dictionary'''
    with open(str(filename) + '.json', 'w') as file:
        json.dump(dict,
                  file)


def create_csv_file(filename: str, df):
    '''convert pandas df to csv file'''
    pd.to_csv(str(filename) + '.csv',
              df,
              sep=',',
              index=False)


class ProductProvider(BaseProvider):
    '''create new faker provdider through class inheritance to generate fake product types'''

    def product(self):
        products = (
            'tshirt pants jeans button-down shorts underwear socks jacket sunglasses hat cap beanie').split(' ')

        # return random selection from products
        return random.choice(products)


# intialize Faker
fake = Faker('en_US')

# create customer database values
num_customer = 10000
customer_dict = {}

for customer in range(num_customer):
    # create customer name, address, DOB, email for DB
    # faker customer name
    fname, lname = fake.first_name(), fake.last_name()
    name = fname + ' ' + lname

    # update dictionary with each entry
    customer_dict.update({customer: {'CustID': fake.ean(length=13),
                                     'name': name,
                                     'street address': fake.street_address(),
                                     'city': fake.city(),
                                     'state': fake.state(),
                                     'post code': fake.postcode(),
                                     'DOB': str(fake.date_of_birth(minimum_age=14, maximum_age=110)),
                                     'email': fname[0] + lname + '@' + fake.free_email_domain()
                                     }
                          }
                         )


# create product order database

# add products to our faker object
fake.add_provider(ProductProvider)
order_dict = {}

for customer in range(num_customer):
    for i in range(random.randint(1, 4)):
        # define random number of items a customer purchases
        order_dict.update({fake.ean(length=8, prefixes=('000')): {'CustID': customer_dict[customer]['CustID'],
                                                                  'product': fake.product(),
                                                                  'item_cost': round(random.uniform(0, 100), 2),
                                                                  'order_time': str(fake.date_time_between(start_date='-1y', end_date='now'))
                                                                  }
                           }
                          )


# create  Iphone user event database
columns = ('EventID CustID ToApp AppOpenTime').split(' ')
event_df = pd.DataFrame(columns=columns, index=None)
num_events = 50000

for event in range(num_events):
    customer_val = random.randint(0, num_customer - 1)
    temp = [[event, customer_dict[customer_val]['CustID'], round(random.uniform(0, 3600), 2), str(
        fake.date_time_between(start_date='-1y', end_date='now'))]]
    event_df = event_df.append(pd.DataFrame(temp, columns=columns), ignore_index=False)

# save dictionaries to file for later import to database
create_json_file('data/customer_db', customer_dict)
create_json_file('data/order_db', order_dict)
create_csv_file('data/event_db',event_df)
