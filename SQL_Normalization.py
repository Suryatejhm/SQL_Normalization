### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error
import datetime


def create_connection(db_file, delete_db=False):
    import os

    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):

    if drop_table_name:  # You can optionally pass drop_table_name to drop the table.
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows


def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    create_table_sql = """CREATE TABLE Region (
        RegionID INTEGER NOT NULL PRIMARY KEY,
        Region TEXT NOT NULL );"""
    insert_regions_sql = """INSERT INTO Region (Region) VALUES ('{}');"""

    create_table(conn, create_table_sql)

    data = pd.read_csv(data_filename, on_bad_lines="skip", header=0, sep="\t")

    regions = set()

    for i in data.index:
        regionName = data["Region"][i]
        print(regionName)
        regions.add(regionName)

    regions = sorted(list(regions))

    for region in regions:
        execute_sql_statement(insert_regions_sql.format(region), conn)
    conn.commit()
    conn.close()

    ### END SOLUTION


def step2_create_region_to_regionid_dictionary(normalized_database_filename):

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    res = {}
    df = pd.read_sql_query("select * from Region;", conn)
    for i in df.index:
        res[df["Region"][i]] = df["RegionID"][i]
    conn.close()
    return res

    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ### BEGIN SOLUTION
    region_to_regionid_dict = step2_create_region_to_regionid_dictionary(
        normalized_database_filename
    )
    conn = create_connection(normalized_database_filename)
    create_table_sql = """CREATE TABLE Country (
        CountryID INTEGER NOT NULL PRIMARY KEY,
        Country TEXT NOT NULL,
        RegionID INTEGER NOT NULL, FOREIGN KEY(RegionID) REFERENCES Region(RegionID) ON DELETE CASCADE ON UPDATE NO ACTION  );"""
    insert_country_sql = (
        """INSERT INTO Country (Country, RegionID) VALUES ('{}', {});"""
    )
    data = pd.read_csv(data_filename, on_bad_lines="skip", header=0, sep="\t")
    country_region_combos = set()
    for i in data.index:
        country_region_combos.add((data["Country"][i], data["Region"][i]))
    country_region_combos = sorted(list(country_region_combos))
    create_table(conn, create_table_sql, "Country")

    for country, region in country_region_combos:
        execute_sql_statement(
            insert_country_sql.format(country, region_to_regionid_dict[region]), conn
        )
    conn.commit()
    conn.close()

    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    res = {}
    df = pd.read_sql_query("select * from Country;", conn)
    for i in df.index:
        res[df["Country"][i]] = df["CountryID"][i]
    conn.close()
    return res

    ### END SOLUTION


def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    country_to_countryid_dict = step4_create_country_to_countryid_dictionary(
        normalized_database_filename
    )
    conn = create_connection(normalized_database_filename)
    create_table_sql = """CREATE TABLE Customer (
        CustomerID INTEGER NOT NULL PRIMARY KEY,
        FirstName TEXT NOT NULL,
        LastName TEXT NOT NULL,
        Address TEXT NOT NULL,
        City TEXT NOT NULL,
        CountryID INTEGER NOT NULL,
        FOREIGN KEY(CountryID) REFERENCES Country(CountryID) ON DELETE CASCADE ON UPDATE NO ACTION  );"""
    insert_regions_sql = """INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) VALUES ("{}", "{}", "{}", "{}", {});"""
    data = pd.read_csv(data_filename, on_bad_lines="skip", header=0, sep="\t")
    name_country_combos = set()
    for i in data.index:
        full_name = data["Name"][i]
        name_parts = full_name.split()
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:])
        address = data["Address"][i]
        city = data["City"][i]
        name_country_combos.add(
            (first_name, last_name, address, city, data["Country"][i])
        )
    name_country_combos = sorted(
        list(name_country_combos), key=lambda x: x[0] + " " + x[1]
    )
    create_table(conn, create_table_sql, "Customer")
   
    for first_name, last_name, address, city, country in name_country_combos:
        sql_statement = insert_regions_sql.format(
            first_name, last_name, address, city, country_to_countryid_dict[country]
        )
        execute_sql_statement(sql_statement, conn)
    conn.commit()
    conn.close()
    ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    res = {}
    df = pd.read_sql_query("select * from Customer;", conn)
    # print(df)
    for i in df.index:
        res[df["FirstName"][i] + " " + df["LastName"][i]] = df["CustomerID"][i]
    conn.close()
    return res

    ### END SOLUTION


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    create_table_sql = """CREATE TABLE ProductCategory (
        ProductCategoryID INTEGER NOT NULL PRIMARY KEY,
        ProductCategory TEXT NOT NULL,
        ProductCategoryDescription TEXT NOT NULL );"""
    insert_regions_sql = """INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES ("{}", "{}");"""
    data = pd.read_csv(data_filename, on_bad_lines="skip", header=0, sep="\t")
    productcategory_description_combos = set()
    for i in data.index:
        product_categories = data["ProductCategory"][i].split(";")
        product_category_descriptions = data["ProductCategoryDescription"][i].split(";")
        for i in range(len(product_categories)):
            productcategory_description_combos.add(
                (product_categories[i], product_category_descriptions[i])
            )
    productcategory_description_combos = sorted(
        list(productcategory_description_combos)
    )
    create_table(conn, create_table_sql, "ProductCategory")
 
    for (
        product_category,
        product_category_description,
    ) in productcategory_description_combos:
        sql_statement = insert_regions_sql.format(
            product_category, product_category_description
        )
        execute_sql_statement(sql_statement, conn)
    conn.commit()
    conn.close()

    ### END SOLUTION


def step8_create_productcategory_to_productcategoryid_dictionary(
    normalized_database_filename,
):

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    res = {}
    df = pd.read_sql_query("select * from ProductCategory;", conn)
    for i in df.index:
        res[df["ProductCategory"][i]] = df["ProductCategoryID"][i]
    conn.close()
    return res

    ### END SOLUTION


def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ### BEGIN SOLUTION
    productcategory_to_productcategoryid_dict = (
        step8_create_productcategory_to_productcategoryid_dictionary(
            normalized_database_filename
        )
    )
    conn = create_connection(normalized_database_filename)
    create_table_sql = """CREATE TABLE Product (
        ProductID INTEGER NOT NULL PRIMARY KEY,
        ProductName TEXT NOT NULL,
        ProductUnitPrice REAL NOT NULL,
        ProductCategoryID INTEGER NOT NULL,
        FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID) ON DELETE CASCADE ON UPDATE NO ACTION );"""
    insert_regions_sql = """INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES ("{}", "{}", "{}");"""
    data = pd.read_csv(data_filename, on_bad_lines="skip", header=0, sep="\t")
    product_combos = set()
    for i in data.index:
        product_names = data["ProductName"][i].split(";")
        product_unit_prices = data["ProductUnitPrice"][i].split(";")
        product_categories = data["ProductCategory"][i].split(";")
        for i in range(len(product_names)):
            product_combos.add(
                (product_names[i], product_unit_prices[i], product_categories[i])
            )
    product_combos = sorted(list(product_combos))
    create_table(conn, create_table_sql, "Product")
    for name, unit_price, category in product_combos:
        sql_statement = insert_regions_sql.format(
            name, unit_price, productcategory_to_productcategoryid_dict[category]
        )
        execute_sql_statement(sql_statement, conn)
    conn.commit()
    conn.close()

    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    res = {}
    df = pd.read_sql_query("select * from Product;", conn)
    for i in df.index:
        res[df["ProductName"][i]] = df["ProductID"][i]
    conn.close()
    return res

    ### END SOLUTION


def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ### BEGIN SOLUTION

    create_table_sql = """CREATE TABLE IF NOT EXISTS [OrderDetail] (
            [OrderID] INTEGER NOT NULL PRIMARY KEY,
            [CustomerID] INTEGER NOT NULL,
            [ProductID] INTEGER NOT NULL,
            [OrderDate] INTEGER NOT NULL,
            [QuantityOrdered] INTEGER NOT NULL,
            FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID) ON DELETE CASCADE ON UPDATE NO ACTION,
            FOREIGN KEY(ProductID) REFERENCES Product(ProductID) ON DELETE CASCADE ON UPDATE NO ACTION);"""

    insert_regions_sql = """INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES ({}, {}, '{}', {});"""

    customer_to_customerid_dictionary = step6_create_customer_to_customerid_dictionary(
        normalized_database_filename
    )
    product_to_product_id_dictionary = step10_create_product_to_productid_dictionary(
        normalized_database_filename
    )

    header = None
    orders = []

    conn = create_connection(normalized_database_filename)

    with open(data_filename, "r") as file:
        for line in file:
            if not header:
                header = line.strip().split("\t")
                continue

            cust_name = line.strip().split("\t")[0]
            cust_id = customer_to_customerid_dictionary[cust_name]
            prod_name = line.strip().split("\t")[5].split(";")
            prod_id = [product_to_product_id_dictionary[prod] for prod in prod_name]
            order_date = line.strip().split("\t")[10].split(";")
            quantity = line.strip().split("\t")[9].split(";")

            quantity = [int(q) for q in quantity]

            order_date = [
                datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
                for date in order_date
            ]

            temp_cust_id = [cust_id] * len(prod_id)
            orders.extend((list(zip(temp_cust_id, prod_id, order_date, quantity))))

        create_table(conn, create_table_sql, "OrderDetail")

        for cust_id, prod_id, order_date, quantity in orders:
            sql_statement = insert_regions_sql.format(
                cust_id, prod_id, order_date, quantity
            )

            execute_sql_statement(sql_statement, conn)

    conn.commit()
    conn.close()
    ### END SOLUTION


def ex1(conn, CustomerName):

    # Simply, you are fetching all the rows for a given CustomerName.
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID

    ### BEGIN SOLUTION

    sql_statement = """
    SELECT Customer.FirstName || ' ' || Customer.LastName as Name, Product.ProductName, OrderDetail.OrderDate, Product.ProductUnitPrice,OrderDetail.QuantityOrdered, ROUND(OrderDetail.QuantityOrdered * Product.ProductUnitPrice, 2) as Total
    FROM Customer
    JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    WHERE Customer.FirstName || ' ' || Customer.LastName = '{}';
    """.format(
        CustomerName
    )
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex2(conn, CustomerName):

    # Simply, you are summing the total for a given CustomerName.
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID

    ### BEGIN SOLUTION
    sql_statement = """
    SELECT DISTINCT Customer.FirstName || ' ' || Customer.LastName as Name, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 2) as Total
    FROM Customer
    JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    WHERE Customer.FirstName || ' ' || Customer.LastName = '{}'
    GROUP BY Customer.CustomerID;
    """.format(
        CustomerName
    )
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex3(conn):

    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending
    ### BEGIN SOLUTION
    sql_statement = """
    SELECT DISTINCT Customer.FirstName || ' ' || Customer.LastName as Name, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 2) as Total
    FROM Customer
    JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    GROUP BY Customer.CustomerID
    ORDER BY Total DESC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex4(conn):

    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and
    # Region tables.
    # Pull out the following columns.
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT Region.Region, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 2) as Total
    FROM Customer
    JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    JOIN Country ON Customer.CountryID = Country.CountryID
    JOIN Region ON Country.RegionID = Region.RegionID
    GROUP BY Region.Region
    ORDER BY Total DESC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex5(conn):

    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns.
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT Country.Country, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 0) as CountryTotal
    FROM Customer
    JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    JOIN Country ON Customer.CountryID = Country.CountryID
    GROUP BY Country.Country
    ORDER BY CountryTotal DESC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):

    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION
    sql_statement = """
    SELECT Region.Region, Country.Country, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 0) as CountryTotal, Rank() OVER (PARTITION BY Region.Region ORDER BY SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice) DESC) as CountryRegionalRank
    FROM Customer
    JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    JOIN Country ON Customer.CountryID = Country.CountryID
    JOIN Region ON Country.RegionID = Region.RegionID
    GROUP BY Region.Region, Country.Country
    ORDER BY Region.Region;
    """

    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex7(conn):

    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT * FROM ( SELECT Region.Region, Country.Country, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 0) as CountryTotal, Rank() OVER (PARTITION BY Region.Region ORDER BY SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice) DESC) as CountryRegionalRank
    FROM Customer
    JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    JOIN Country ON Customer.CountryID = Country.CountryID
    JOIN Region ON Country.RegionID = Region.RegionID
    GROUP BY Region.Region, Country.Country) WHERE CountryRegionalRank = 1
    ORDER BY Region;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex8(conn):

    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
      SELECT
      CASE 
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 4 AND 6 THEN 'Q2'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 7 AND 9 THEN 'Q3'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 10 AND 12 THEN 'Q4'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 1 AND 3 THEN 'Q1'
      END Quarter, CAST(strftime('%Y', ORDERDATE) AS INT) Year , Customer.CustomerID, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 0) as Total FROM OrderDetail
      JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
      JOIN Product ON OrderDetail.ProductID = Product.ProductID
      GROUP BY Quarter, Year, Customer.CustomerID
      ORDER BY Year, Quarter;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex9(conn):

    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
      SELECT t2.* FROM ( SELECT t1.*, Rank() OVER (PARTITION BY Year, Quarter ORDER BY TOTAL DESC) as CustomerRank FROM ( SELECT
      CASE 
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 4 AND 6 THEN 'Q2'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 7 AND 9 THEN 'Q3'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 10 AND 12 THEN 'Q4'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) BETWEEN 1 AND 3 THEN 'Q1'
      END Quarter, CAST(strftime('%Y', ORDERDATE) AS INT) Year , Customer.CustomerID, ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 0) as Total FROM OrderDetail
      JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
      JOIN Product ON OrderDetail.ProductID = Product.ProductID
      GROUP BY Quarter, Year, Customer.CustomerID) t1 ) t2 WHERE CustomerRank <= 5
      ORDER BY Year, Quarter;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex10(conn):

    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """
      SELECT t2.* FROM ( SELECT t1.*, Rank() OVER ( ORDER BY TOTAL DESC ) as TotalRank FROM ( SELECT
      CASE
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 1 THEN 'January'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 2 THEN 'February'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 3 THEN 'March'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 4 THEN 'April'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 5 THEN 'May'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 6 THEN 'June'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 7 THEN 'July'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 8 THEN 'August'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 9 THEN 'September'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 10 THEN 'October'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 11 THEN 'November'
      WHEN CAST(strftime('%m', ORDERDATE) AS INT) = 12 THEN 'December'
      END
      Month, SUM(ROUND(OrderDetail.QuantityOrdered * Product.ProductUnitPrice)) AS Total FROM OrderDetail
      JOIN Product ON OrderDetail.ProductID = Product.ProductID
      GROUP BY Month) t1
      ) t2
    ORDER BY TotalRank;
    """

    ### END SOLUTION ROUND(SUM(OrderDetail.QuantityOrdered * Product.ProductUnitPrice), 1)
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex11(conn):

    # Find the MaxDaysWithoutOrder for each customer
    # Output Columns:
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate,
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
     SELECT t2.CustomerID, t2.FirstName, t2.LastName, t2.Country, t2.OrderDate, t2.PreviousOrderDate, ROUND(t2.MaxDaysWithoutOrder, 0) MaxDaysWithoutOrder FROM ( SELECT t3.*, RANK() OVER ( PARTITION BY CustomerID ORDER BY t3.OrderDate ) RN1 FROM ( SELECT t4.* FROM ( SELECT t1.*, RANK() OVER ( PARTITION BY CustomerID ORDER BY MaxDaysWithoutOrder DESC ) RN FROM ( SELECT Customer.CustomerID, FirstName, LastName, Country.Country, OrderDetail.OrderDate, Lag(OrderDetail.OrderDate) OVER (PARTITION BY Customer.CustomerID ORDER BY OrderDetail.OrderDate) as PreviousOrderDate, CAST((julianday(OrderDetail.OrderDate) - julianday(Lag(OrderDetail.OrderDate) OVER (PARTITION BY Customer.CustomerID ORDER BY OrderDetail.OrderDate))) AS INT) as MaxDaysWithoutOrder
     FROM Customer
     JOIN Country ON Customer.CountryID = Country.CountryID
     JOIN OrderDetail ON Customer.CustomerID = OrderDetail.CustomerID 
     )
     t1
     WHERE MaxDaysWithoutOrder IS NOT NULL ) t4 
     WHERE RN = 1 ) t3) t2
     WHERE RN1 = 1
     ORDER BY MaxDaysWithoutOrder DESC, t2.CustomerID DESC
     ;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement
