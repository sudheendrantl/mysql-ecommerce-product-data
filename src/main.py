import database as db

# Driver code
if __name__ == "__main__":
    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = "Ragh1008!18"  # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerce_record"  # This is the name of the db we will create in next step - call it whatever you like.
    LOCALHOST = "localhost"

    # create a connection to the DB
    connection = db.create_db_connection(LOCALHOST,ROOT,PW,DB)

    # Start implementing your task as mentioned in the problem statement 
    # Implement all the test cases and test them by running this file

    #Problem step 2b
    # create a list with different/unique customer IDs by choosing random but
    # valid values of customer_id, vendor_id, total_value, order_quantity and reward_point from orders table
    sql = "SELECT DISTINCT customer_id FROM orders ORDER BY rand() limit 5;"
    cust_ids = db.select_query(connection, sql)

    sql = "SELECT DISTINCT vendor_id FROM orders ORDER BY rand() limit 5;"
    vendor_ids = db.select_query(connection, sql)

    sql = "SELECT DISTINCT total_value FROM orders ORDER BY rand() limit 5;"
    total_values = db.select_query(connection, sql)

    sql = "SELECT DISTINCT order_quantity FROM orders ORDER BY rand() limit 5;"
    order_quantities = db.select_query(connection, sql)

    sql = "SELECT DISTINCT reward_point FROM orders ORDER BY rand() limit 5;"
    reward_points = db.select_query(connection, sql)

    # get the max order_id in the database, so that we can
    # incrementally add/append higher values of order_ids into the table
    sql = "SELECT MAX(order_id) FROM orders;"
    max_id = db.select_query(connection, sql)[0][0]

    print("\n\nInserting 5 records in the order table (for problem statement 2.b) ...")
    for i in range(0,5):
        # new order_id = max + i + 1; cust[i] chooses one of the valid customer IDs from the list
        val = str(max_id + i + 1) + "," + str(cust_ids[i][0])+ "," + \
                                        str(vendor_ids[i][0]) + "," + \
                                        str(total_values[i][0]) + "," + \
                                        str(order_quantities[i][0]) + ","+ \
                                        str(reward_points[min(i,2) ][0])
        sql = (f"""INSERT INTO orders (order_id,customer_id,vendor_id,total_value,
                    order_quantity,reward_point) VALUES({val});""")
        print(f"\n\nInserting ({val}) ...")
        db.create_insert_query(connection, sql)

    #Problem step 2c

    # before printing the records, print the column headings for ease of viewing on the console
    print(f"\n\nPrinting all the records from orders table (for problem statement 2.c) ...")

    sql = "SHOW COLUMNS FROM orders;"
    result = db.select_query(connection, sql)
    print("---------------------------------------------------------------------------------------------")
    print("{:<15} {:<15} {:<15} {:<15} {:<15} {:<15}".format(result[0][0], result[1][0], result[2][0], result[3][0], result[4][0], result[5][0]))
    print("---------------------------------------------------------------------------------------------")

    # get all data from the orders table
    sql = "SELECT * FROM orders;"
    result = db.select_query(connection, sql)

    # now print the records
    for item in result:
        (v1, v2, v3, v4, v5, v6) = item
        print("{:<15} {:<15} {:<15} {:<15} {:<15} {:<15}".format(v1, v2, v3, v4, v5, v6))

    print("---------------------------------------------------------------------------------------------")

    #Problem step 3a
    # find the maximum order value in the total_value column of the orders database
    # and print on the console
    sql = "SELECT MAX(total_value) FROM orders;"
    max_value = db.select_query(connection, sql)[0][0]
    print(f"\n\nThe maximum order value found in the orders table is :{max_value} (for problem statement 3.a) ... ")

    # print the entire record with the maximum value
    sql = f"SELECT * FROM orders WHERE total_value = (SELECT MAX(total_value) FROM orders);"
    max_value_record = db.select_query(connection, sql)
    print(f"\n\nThe record(s) with maximum order value found in the orders table is/are : (for problem statement 3.a)")
    for item in max_value_record:
        print(item)

    print("---------------------------------------------------------------------------------------------")

    # find the minimum order value in the total_value column of the orders database
    # and print on the console
    sql = "SELECT MIN(total_value) FROM orders;"
    min_value = db.select_query(connection, sql)[0][0]
    print(f"\n\nThe minimum order value found in the orders table is :{min_value} (for problem statement 3.a) ... ")

    # print the entire record with the minimum value
    sql = f"SELECT * FROM orders WHERE total_value = (SELECT MIN(total_value) FROM orders);"
    min_value_record = db.select_query(connection, sql)
    print(f"\n\nThe record(s) with minimum order value found in the orders table is/are : (for problem statement 3.a)")
    for item in min_value_record:
        print(item)

    print("---------------------------------------------------------------------------------------------")

    #Problem step 3b
    # find the average order value in the total_value column of the orders database
    sql = "SELECT AVG(total_value) FROM orders;"
    avg_value = db.select_query(connection, sql)[0][0]

    # before printing the records, lets get the count of such records where the order_value is higher than
    # the average order value
    sql = f"SELECT COUNT(*) FROM orders WHERE total_value > {avg_value};"
    count_above_avg = db.select_query(connection, sql)

    # before printing the records, print the column headings for ease of viewing on the console
    sql = "SHOW COLUMNS FROM orders;"
    result = db.select_query(connection, sql)
    print(f"\n\nPrinting {count_above_avg[0][0]} records from orders table where order_value is higher than the average order value of: {avg_value} (for problem statement 3.b)...")
    print("---------------------------------------------------------------------------------------------")
    print("{:<15} {:<15} {:<15} {:<15} {:<15} {:<15}".format(result[0][0], result[1][0], result[2][0], result[3][0], result[4][0], result[5][0]))
    print("---------------------------------------------------------------------------------------------")

    # fetch the orders where order value is greater than the average
    # and print to the console
    sql = f"SELECT * FROM orders WHERE total_value > {avg_value};"
    result = db.select_query(connection, sql)
    for item in result:
        (v1, v2, v3, v4, v5, v6) = item
        print("{:<15} {:<15} {:<15} {:<15} {:<15} {:<15}".format(v1, v2, v3, v4, v5, v6))

    print("---------------------------------------------------------------------------------------------")

    #Problem step 3c

    # drop any existing customer_leaderboard_table, if it exists
    print(f"\n\nDropping old/existing customer_leaderboard table before creating afresh (for problem statement 3.c) ...")
    sql = "DROP TABLE IF EXISTS customer_leaderboard;"
    db.drop_table(connection, sql)

    # create a new customer_leaderboard table as per spec
    customer_leaderboard_table = """
        	CREATE TABLE customer_leaderboard (
        	  customer_id varchar(10) PRIMARY KEY,
        	  total_value float NOT NULL,
        	  customer_name varchar(50) NOT NULL,
        	  customer_email varchar(50) NOT NULL
        	)
        	"""

    print("\n\nCreating a new table in the name of customer_leaderboard (for problem statement 3.c) ...")
    db.create_table(connection, customer_leaderboard_table)

    # get unique row for each customer using GROUP BY clause and
    # get the total_value with the max value of purchase made by the customer
    # as we need customer_name and customer_email from users table,
    # we get it by joining with users table which has user_name and user_email instead
    sql = """SELECT orders.customer_id, MAX(orders.total_value), users.user_name, users.user_email
            FROM orders
            INNER JOIN users
            ON orders.customer_id = users.user_id
            WHERE users.is_vendor = 0 
            GROUP BY orders.customer_id;"""

    result = db.select_query(connection, sql)

    # insert every row into the customer_leaderboard table, using the iterator on the above result
    print(f"\n\nInserting 1 unique record for each customer who has placed atleast one order (i.e. {len(result)} records) in the customer_leaderboard table (for problem statement 3.c) ...")
    for item in result:
        sql = f"""INSERT INTO `customer_leaderboard` (`customer_id`, `total_value`, `customer_name`, `customer_email`) 
                    VALUES ({item[0]},{item[1]},'{item[2]}','{item[3]}');"""
        db.create_insert_query(connection, sql)

    # fetch the orders where order value is greater than the average
    # and print to the console
    sql = "SHOW COLUMNS FROM customer_leaderboard;"
    result = db.select_query(connection, sql)
    print(f"\n\nPrinting customer_leaderboard table (for problem statement 3.c)...")
    print("---------------------------------------------------------------------------------------------")
    print("{:<15} {:<15} {:<15} {:<15}".format(result[0][0], result[1][0], result[2][0], result[3][0]))
    print("---------------------------------------------------------------------------------------------")

    sql = f"SELECT * FROM customer_leaderboard;"
    result = db.select_query(connection, sql)
    for item in result:
        (v1, v2, v3, v4) = item
        print("{:<15} {:<15} {:<15} {:<15}".format(v1, v2, v3, v4))

    print("---------------------------------------------------------------------------------------------")

    # close the db connection amd gracefully exit
    print("\n\nTerminating all connections and closing out...")
    connection.close()
    connection.disconnect()
    connection.shutdown()
