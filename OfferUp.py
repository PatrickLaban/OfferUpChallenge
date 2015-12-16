"""
OfferUp Interview Challenge
Author - Patrick Laban
Contact - laban.patrick@gmail.com

This module is for the OfferUp Interview.  Please do not use this if you have received the challenge.

A quick note - some of the extra commenting here is to help explain my reasoning, not necessarily the code itself.

A few assumptions -
This is meant to be interview code and not run in a production environment.  Therefore it is not
built to scale up without modification.

Security was considered but is only focused on preventing sql injection
and not storing passwords in a public repo.

While there is some error handling it is minimal and there mostly to prevent the database connection from being locked.

"""

import os
import logging
import psycopg2
from flask import Flask
from flask_restful import Resource, Api, reqparse
from werkzeug.contrib.cache import SimpleCache

# Using SimpleCache to avoid the overhead of running a memcache server
# I considered just using a dictionary, but wanted TTL functionality
price_cache = SimpleCache()

# Used for when the item is not supplied
ERROR_RESPONSE = {
    "status": 404,
    "content": {
        "message": "Not found"
    }
}

# Connection info - Stored within system variables so they are not shared on GitHub
DB_NAME = os.environ['OU_DBNAME']
HOST = os.environ['OU_HOST']
PORT = os.environ['OU_PORT']
USER = os.environ['OU_USER']
DB_PASSWORD = os.environ['OU_PASSWORD']

# Parser is a simple HTTP argument parser within Flask-RESTful
# We are only expecting item and city within the arguments
parser = reqparse.RequestParser()
parser.add_argument('item', type=str)
parser.add_argument('city', type=str)

# We only need to establish the connection once, so lets do it at the top level of the module
connection = psycopg2.connect(dbname=DB_NAME, password=DB_PASSWORD, host=HOST, user=USER, port=PORT)
cursor = connection.cursor()


class ItemPriceService(Resource):
    def get(self):
        """
        Handles get requests to the /item-price-service/ end point.  The arguments that may be in the url params are
        city and item.  Item is what we want to provide a recommended price for.  This price is the mode of the list
        prices (possibly for a city or for all areas) from our price database.  City is not required.  If it is
        provided then the recommended price will be based off of the mode for that city.

        Later on we may want to update this formula to take the sell price into account as well.

        :return:
        A json response of the following form -
        {
            "status": 200,
            "content": {
                "item": "Furniture",
                "item_count": 6,
                "price_suggestion": 48,
                "city": "Philadelphia"
            }
        }

        If city is not provided then the city field will be set to "Not specified".
        In the case of the item not being provided return the following -
        {
            "status": 404,
            "content": {
                "message": "Not found"
            }
        }

        """
        args = parser.parse_args()
        city = args['city']
        item = args['item']
        if item is None:
            return ERROR_RESPONSE
        # Get our cache key and set the city string based on if city is in the request args
        if city is None:
            price_key = item
            city_str = "Not specified"
        else:
            price_key = city + item
            city_str = city
        price = price_cache.get(price_key)
        if price is None:
            # First time we've seen this request or it has expired, qurey for it from the db
            item_count, mode = self.query_item_price_db(city, item)
            if item_count == 0:
                # Special case - this is a valid query but there were no results, so we can't make a price recomendation
                return ERROR_RESPONSE
            price = {
                "status": 200,
                "content": {
                    "item": item,
                    "item_count": item_count,
                    "price_suggestion": mode,
                    "city": city_str
                }
            }
            # Cache our response object with TTL of 5 minutes
            price_cache.set(price_key, price, timeout=5 * 60)
        return price

    def query_item_price_db(self, city, item):
        """
        Builds the query strings taking into account if a city was provided.
        :param item:
        The item we are getting a price recommendation for
        :param city:
        If provided the city to look at for list prices
        :return:
        A tuple of the form (item count, list price mode)

        """

        if city and item:
            city_clause = " AND city=%s"
            query_data = (item, city)
        else:
            city_clause = ""
            query_data = (item,)

        count_query_string = '\
            SELECT COUNT(list_price)\
            FROM "itemPrices_itemsale"\
            WHERE title=%s\
            {city_clause};\
        '.format(city_clause=city_clause)

        mode_query_string = '''
            SELECT list_price, COUNT(list_price) AS frequency
            FROM "itemPrices_itemsale"
            WHERE title=%s\
            {city_clause}\
            GROUP BY list_price
            ORDER BY COUNT(list_price) DESC, list_price DESC
            LIMIT 1
        '''.format(city_clause=city_clause)

        count = self.query_db(count_query_string, query_data)[0]
        if count == 0:
            # No point in running mode query since there wont be any results
            return 0, None
        mode = self.query_db(mode_query_string, query_data)[0]
        return count, mode

    def query_db(self, query_string, query_data):
        """
        Does the actual query to the postgres db we connected to earlier.  Note that this only calls fetchone, so
        a new function will be needed if we care about multiple rows in our results.

        If we encounter a programming error then we rollback the connection so that we can continue to make queries.

        :param query_string:
        The query to run.
        :param query_data:
        The data to fill into the blanks of our query string.  psycopg2 does this to prevent sql injection
        :return:
        The results from fetchone()
        """
        try:
            cursor.execute(query_string, query_data)
            return cursor.fetchone()
        except psycopg2.ProgrammingError as e:
            connection.rollback()
            logging.ERROR('DB Programming Error')
            raise e

app = Flask(__name__)
api = Api(app)
api.add_resource(ItemPriceService, '/item-price-service/')

if __name__ == '__main__':
    app.run(debug=False)