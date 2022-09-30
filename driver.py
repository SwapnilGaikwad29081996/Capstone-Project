# Streaming Application to read from Kafka
# This is the driver file for your project
#importing necessary libraries
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import happybase
import math
import pandas as pd
from datetime import datetime
import uuid


class GEO_Map():
    """
    It hold the  map for zip code and its latitute and longitute
    """
    __instance = None

    @staticmethod
    def get_instance():
        """ Static access method. """
        if GEO_Map.__instance == None:
            GEO_Map()
        return GEO_Map.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if GEO_Map.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            GEO_Map.__instance = self
            self.map = pd.read_csv("uszipsv.csv", header=None, names=['A','B','C','D','E'])
            self.map['A'] =  self.map['A'].astype(str)

    def get_lat(self, pos_id):
        return self.map[self.map.A == pos_id ].B

    def get_long(self, pos_id):
        return self.map[self.map.A == pos_id ].C

    def distance(self, lat1, long1, lat2, long2):
        theta = long1 - long2
        dist = math.sin(self.deg2rad(lat1)) * math.sin(self.deg2rad(lat2)) + math.cos(self.deg2rad(lat1)) * math.cos(self.deg2rad(lat2)) * math.cos(self.deg2rad(theta))
        dist = math.acos(dist)
        dist = self.rad2deg(dist)
        dist = dist * 60 * 1.1515 * 1.609344
        return dist

    def rad2deg(self, rad):
        return rad * 180.0 / math.pi

    def deg2rad(self, deg):
        return deg * math.pi / 180.0


class HBaseDao:
    """
    Dao class for operation on HBase
    """
    __instance = None

    @staticmethod
    def get_instance():
        """ Static access method. """
        if HBaseDao.__instance == None:
            HBaseDao()
        return HBaseDao.__instance

    def __init__(self):
        if HBaseDao.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            HBaseDao.__instance = self
            'localhost' = '100.25.111.148'
            for i in range(2):
                try:
                    self.pool = happybase.ConnectionPool(size=3, host=self.host, port=9090)
                    break
                except:
                    print("Exception in connecting HBase")


    def get_data(self, key, table):
        for i in range(2):
            try:
                with self.pool.connection() as connection:
                    t = connection.table(table)
                    row = t.row(key)
                    return row
            except:
                self.reconnect()



    def write_data(self, key, row, table):
        for i in range(2):
            try:
                with self.pool.connection() as connection:
                    t = connection.table(table)
                    t.put(key, row)
            except:
                self.reconnect()

    def reconnect(self):
        self.pool = happybase.ConnectionPool(size=3, host=self.host)


# List all the functions to check for the rules


# Create UDF functions
lookup_table = 'lookup_data_hbase'
master_table = 'card_transactions_hbase'
speed_threshold = 0.25  # km/sec - Average speed of flight 900 km/hr

"""
Function to verify the UCL rule
Transaction amount should be less than Upper control limit (UCL)
:param card_id: (Long) Card id of the card customer
:param amount: (Double) The transaction amount
:return: (Boolean)
"""
def verify_ucl_data(card_id, amount):
    try:
        hbasedao = HBaseDao.get_instance()

        card_row = hbasedao.get_data(key=str(card_id), table=lookup_table)
        card_ucl = (card_row[b'card_data:ucl']).decode("utf-8")

        if amount < float(card_ucl):
            return True
        else:
            return False
    except Exception as e:
        raise Exception(e)

"""
Function to verify the credit score rule
Credit score of each member should be greater than 200
:param card_id: (Long) Card id of the card customer
:param score: (Integer) Credit score of the card user
:return: (Boolean)
"""
def verify_credit_score_data(card_id):

    try:
        hbasedao = HBaseDao.get_instance()

        card_row = hbasedao.get_data(key=str(card_id), table=lookup_table)
        card_score = (card_row[b'card_data:score']).decode("utf-8")

        if int(card_score) > 200:
            return True
        else:
            return False
    except Exception as e:
        raise Exception(e)

"""
Function to verify the following zipcode rules
ZIP code distance
:param card_id: (Long) Card id of the card customer
:param postcode: (Integer) Post code of the card transaction
:param transaction_dt: (String) Timestamp
:return: (Boolean)
"""
def verify_postcode_data(card_id, postcode, transaction_dt):

    try:
        hbasedao = HBaseDao.get_instance()
        geo_map = GEO_Map.get_instance()

        card_row = hbasedao.get_data(key=str(card_id), table=lookup_table)
        last_postcode = (card_row[b'card_data:postcode']).decode("utf-8")
        last_transaction_dt = (card_row[b'card_data:transaction_dt']).decode("utf-8")

        current_lat = geo_map.get_lat(str(postcode))
        current_lon = geo_map.get_long(str(postcode))
        previous_lat = geo_map.get_lat(last_postcode)
        previous_lon = geo_map.get_long(last_postcode)

        dist = geo_map.distance(lat1=current_lat, long1=current_lon, lat2=previous_lat, long2=previous_lon)

        speed = calculate_speed(dist, transaction_dt, last_transaction_dt)

        if speed < speed_threshold:
            return True
        else:
            return False

    except Exception as e:
        raise Exception(e)

"""
A function to calculate the speed from distance and transaction timestamp differentials
:param dist: (Float) Distance between postcodes
:param transaction_dt1: Transaction timestamp from the table
:param transaction_dt2: Transaction timestamp from the POS
:return: (Float) Speed
"""
def calculate_speed(dist, transaction_dt1, transaction_dt2):

    transaction_dt1 = datetime.strptime(transaction_dt1, '%d-%m-%Y %H:%M:%S')
    transaction_dt2 = datetime.strptime(transaction_dt2, '%d-%m-%Y %H:%M:%S')

    elapsed_time = transaction_dt1 - transaction_dt2
    elapsed_time = elapsed_time.total_seconds()

    try:
        return dist / elapsed_time
    except ZeroDivisionError:
        return 299792.458
# (Speed of light)

"""
A function to verify all the three rules - ucl, credit score and speed
:param card_id: (Long) Card id of the card customer from POS
:param member_id: (Long) Member id of the card customer from POS
:param amount: (Integer) Transaction amount from POS
:param pos_id: (Long) Transaction position id from POS
:param postcode: (Integer) Post code of the card transaction from POS
:param transaction_dt: (String) Transaction timestamp from POS
:return: (String) Status of the transaction
"""
def verify_rules_status(card_id, member_id, amount, pos_id, postcode, transaction_dt):


    hbasedao = HBaseDao.get_instance()

    # Check if the POS transaction passes all rules.
    # If yes, update the lookup table and insert data in master table as genuine.
    # Else insert the transaction in master table as Fraud.

    rule1 = verify_ucl_data(card_id, amount)
    rule2 = verify_credit_score_data(card_id)
    rule3 = verify_postcode_data(card_id, postcode, transaction_dt)

    if all([rule1, rule2, rule3]):
        status = 'GENUINE'
        hbasedao.write_data(key=str(card_id),
                            row={'card_data:postcode': str(postcode), 'card_data:transaction_dt': str(transaction_dt)},
                            table=lookup_table)
    else:
        status = 'FRAUD'

    new_id = str(uuid.uuid4()).replace('-', '')
    hbasedao.write_data(key=new_id,
                        row={'cardDetail:card_id': str(card_id), 'cardDetail:member_id': str(member_id),
                             'transactionDetail:amount': str(amount), 'transactionDetail:pos_id': str(pos_id),
                             'transactionDetail:postcode': str(postcode), 'transactionDetail:status': str(status),
                             'transactionDetail:transaction_dt': str(transaction_dt)},
                        table=master_table)
    return status

#initialising Spark session    
spark = SparkSession  \
    .builder  \
    .appName("CreditCardFraud")  \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading input from Kafka
credit_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("startingOffsets","earliest") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "transactions-topic-verified") \
    .load()

# Defining schema for transaction
dataSchema = StructType() \
    .add("card_id", LongType()) \
    .add("member_id", LongType()) \
    .add("amount", DoubleType()) \
    .add("pos_id", LongType()) \
    .add("postcode", IntegerType()) \
    .add("transaction_dt", StringType())

# Casting raw data as string and aliasing
credit_data = credit_data.selectExpr("cast(value as string)")
credit_data_stream = credit_data.select(from_json(col="value", schema=dataSchema).alias("credit_data")).select(
    "credit_data.*")

# Define UDF which verifies all the rules for each transaction and updates the lookup and master tables
verify_all_rules = udf(verify_rules_status, StringType())

Final_data = credit_data_stream \
    .withColumn('status', verify_all_rules(credit_data_stream['card_id'],
                                           credit_data_stream['member_id'],
                                           credit_data_stream['amount'],
                                           credit_data_stream['pos_id'],
                                           credit_data_stream['postcode'],
                                           credit_data_stream['transaction_dt']))

# Write output to console as well
output_data = Final_data \
    .select("card_id", "member_id", "amount", "pos_id", "postcode", "transaction_dt") \
    .writeStream \
    .trigger(processingTime = '1 seconds') \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

#indicating Spark to await termination
output_data.awaitTermination()
