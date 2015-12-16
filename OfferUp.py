import os

import psycopg2
from flask import Flask
from flask_restful import Resource, Api, reqparse
from werkzeug.contrib.cache import SimpleCache

price_cache = SimpleCache()
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


class ItemPriceService(Resource):
    def get(self):
        args = parser.parse_args()
        city = args['city']
        item = args['item']
        if item is None:
            return ERROR_RESPONSE
        if city is None:
            price_key = item
            city_str = "Not specified"
        else:
            price_key = city + item
            city_str = city
        price = price_cache.get(price_key)
        if price is None:
            item_count, mode = self.query_item_price_db(city, item)
            price = {
                "status": 200,
                "content": {
                    "item": item,
                    "item_count": item_count,
                    "price_suggestion": mode,
                    "city": city_str
                }
            }
            price_cache.set(price_key, price, timeout=5 * 60)
        return price

    def query_item_price_db(self, city, item):
        connection = psycopg2.connect(dbname=DB_NAME, password=DB_PASSWORD, host=HOST, user=USER, port=PORT)
        cursor = connection.cursor()
        if city and item:
            city_clause = " AND city=%s"
            sql_data = (item, city)
        else:
            city_clause = ""
            sql_data = (item,)
        count_query_string = '\
            SELECT COUNT(sell_price)\
            FROM "itemPrices_itemsale"\
            WHERE title=%s\
            {city_clause};\
        '.format(city_clause=city_clause)
        mode_query_string = '''
            SELECT sell_price, COUNT(sell_price) AS frequency
            FROM "itemPrices_itemsale"
            WHERE title=%s\
            {city_clause}\
            GROUP BY sell_price
            ORDER BY COUNT(sell_price) DESC, sell_price DESC
            LIMIT 1
        '''.format(city_clause=city_clause)
        cursor.execute(count_query_string, sql_data)
        count_results = cursor.fetchone()
        count = count_results[0]
        if count == 0:
            # No point in running mode query
            return None, None
        cursor.execute(mode_query_string, sql_data)
        mode_results = cursor.fetchone()
        mode = mode_results[0]
        return count, mode

app = Flask(__name__)
api = Api(app)
api.add_resource(ItemPriceService, '/item-price-service/')

if __name__ == '__main__':
    app.run(debug=True)