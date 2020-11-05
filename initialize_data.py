'''
Python script to create fake database of users and purchase orders for use in ETL pipeline
'''
from faker import Faker
import numpy as np
import random
import json
from faker.providers import BaseProvider

# ---- function defintions -------

def create_json_file(filename:str,dict):
    '''function used to dump json file from dictionary'''
    with open(str(filename)+'.json', 'w') as file:
        json.dump(dict, file)

class ProductProvider(BaseProvider):
    '''create new faker class to generate product types'''
    def product(self):
        products = ('tshirt pants jeans button-down shorts underwear socks jacket sunglasses hat cap beanie').split(' ')

        # return random selection from products
        return random.choice(products)

# intialize Faker
fake = Faker('en_US')

# create customer database values
num_entry = 10
customers = {}

for entry in range(num_entry):
    # create customer name, address, DOB, email for DB
    # faker customer name
    fname, lname = fake.first_name(), fake.last_name()
    name = fname + ' ' + lname

    # update dictionary with each entry
    customers.update({entry: {'CustID': fake.ean(length=13),
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
    if entry == (num_entry-1):
        create_json_file('customer_db',customers)

# create product order database

# add products to our faker object
fake.add_provider(ProductProvider)
