import argparse
from time import time

import mysql.connector


class DataBaseManager:
    def __init__(self, user, password, host, database, query):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.query = query
        self.cnx = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database)
        self.cnx.disconnect()

    def connect(self) -> float:
        """Connect to database"""
        t = time()
        self.cnx.connect()
        return time() - t

    def run_query(self) -> float:
        t = time()
        cursor = self.cnx.cursor(buffered=False)
        cursor.execute(self.query)
        cursor.fetchall()
        return time() - t

    def disconnect(self) -> float:
        t = time()
        self.cnx.disconnect()
        return time() - t

    def reconnect(self) -> float:
        t = time()
        self.cnx.reconnect()
        return time() - t


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('username', type=str, help='Username to database')
    parser.add_argument('password', type=str, help='Password to database')
    parser.add_argument('host', type=str, help='Host of database')
    parser.add_argument('schema', type=str, help='Schema in database')
    parser.add_argument('query', type=str, help='Query to perform in database')
    parser.add_argument('-s', '--steps', type=int, help='How many steps to calculate average', default=100)
    args = parser.parse_args()

    db = DataBaseManager(args.username, args.password, args.host, args.schema, args.query)
    # new connection per query
    total_time_for_new_connection = db.connect()
    for _ in range(args.steps):
        total_time_for_new_connection += db.reconnect()
        total_time_for_new_connection += db.run_query()
    total_time_for_new_connection += db.disconnect()
    print('Average time need for query using new connections:', total_time_for_new_connection / args.steps)

    # same connection to all queries
    total_time_for_same_connection = db.connect()
    for _ in range(args.steps):
        total_time_for_same_connection += db.run_query()
    total_time_for_same_connection += db.disconnect()
    print('Average time need for query using same connection:', total_time_for_same_connection / args.steps)
