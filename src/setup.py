import csv
import database as db

PW = "Ragh1008!18"  # IMPORTANT! Put your MySQL Terminal password here.
ROOT = "root"
DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever you like.
LOCALHOST = "localhost"  # considering you have installed MySQL server on your computer

RELATIVE_CONFIG_PATH = '../config/'

USER = 'users'
PRODUCTS = 'products'
ORDER = 'orders'

connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB
db.create_and_switch_database(connection, DB, DB)

# Create the tables through python code here
# if you have created the table in UI, then no need to define the table structure
# If you are using python to create the tables, call the relevant query to complete the creation
user_table = """
    	CREATE TABLE users (
    	  user_id varchar(10) PRIMARY KEY,
    	  user_name varchar(45) NOT NULL,
    	  user_email varchar(45) NOT NULL,
    	  user_password varchar(45) NOT NULL,
    	  user_address varchar(45),
    	  is_vendor tinyint(1) NOT NULL
    	)
    	"""

orders_table = """
    	CREATE TABLE orders (
    	  order_id int PRIMARY KEY,
    	  total_value double NOT NULL,
    	  order_quantity int NOT NULL,
    	  reward_point int NOT NULL,
    	  vendor_id varchar(10) NOT NULL,
    	  customer_id varchar(10) NOT NULL, 
    	  CONSTRAINT `fk_vendor_id_orders` FOREIGN KEY (`vendor_id`) REFERENCES `users` (`user_id`),
    	  CONSTRAINT `fK_customer_id_orders` FOREIGN KEY (`customer_id`) REFERENCES `users` (`user_id`)
    	)
    	"""

products_table = """
    	CREATE TABLE products (
    	  product_id varchar(45) PRIMARY KEY,
    	  product_name varchar(45) NOT NULL,
    	  product_description varchar(100) NOT NULL,
    	  product_price double NOT NULL,
    	  emi_available varchar(10) NOT NULL,
    	  vendor_id varchar(10) NOT NULL, 
    	  CONSTRAINT `fk_vendor_id_prd` FOREIGN KEY (`vendor_id`) REFERENCES `users` (`user_id`)
    	)
    	"""

customer_leaderboard_table = """
    	CREATE TABLE customer_leaderboard (
    	  customer_id varchar(10) PRIMARY KEY,
    	  total_value double NOT NULL,
    	  customer_name varchar(50) NOT NULL,
    	  customer_email varchar(50) NOT NULL,
    	  CONSTRAINT `fk_customer_id_leaderboard` FOREIGN KEY (`customer_id`) REFERENCES `users` (`user_id`)
    	)
    	"""

db.create_table(connection, user_table)
db.create_table(connection, orders_table)
db.create_table(connection, products_table)
db.create_table(connection, customer_leaderboard_table)

with open(RELATIVE_CONFIG_PATH + USER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    sql = '''
        INSERT INTO users (user_id, user_name, user_email, user_password, user_address, is_vendor) 
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
    db.insert_many_records(connection,sql,val)

with open(RELATIVE_CONFIG_PATH + PRODUCTS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    sql = '''
        INSERT INTO products (product_id,product_name,product_price,product_description,vendor_id,emi_available) 
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
    db.insert_many_records(connection,sql,val)

with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    sql = '''
        INSERT INTO orders (order_id,customer_id,vendor_id,total_value,order_quantity,reward_point) 
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
    db.insert_many_records(connection,sql,val)
