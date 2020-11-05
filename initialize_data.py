'''
Python script to create fake database of users and purchase orders for use in ETL pipeline
'''
from faker import Faker
import numpy as np
import random
import json
from faker.providers import BaseProvider
from datetime import datetime

# ---- function defintions -------


def create_json_file(filename: str, dict):
    '''function used to dump json file from dictionary'''
    with open(str(filename) + '.json', 'w') as file:
        json.dump(dict, file)


class ProductProvider(BaseProvider):
    '''create new faker class to generate product types'''

    def product(self):
        products = (
            'tshirt pants jeans button-down shorts underwear socks jacket sunglasses hat cap beanie').split(' ')

        # return random selection from products
        return random.choice(products)


# intialize Faker
fake = Faker('en_US')

# create customer database values
num_customer = 3
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
        order_dict.update({fake.ean(length=8, prefixes=('000')): {'CustID': customer_dict[customer]['CustID'],
                                                                  'product': fake.product(),
                                                                  'item_cost': round(random.uniform(0, 100), 2),
                                                                  'order_time': str(fake.date_time_between(start_date='-1y', end_date='now'))
                                                                  }
                           }
                          )


# save dictionaries to json file for later import to database
create_json_file('data/customer_db', customer_dict)
create_json_file('data/order_db', order_dict)
