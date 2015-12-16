from flask import Flask
from flask_restful import Resource, Api, reqparse
from werkzeug.contrib.cache import SimpleCache
import psycopg2
import os


price_cache = SimpleCache()
ERROR_RESPONSE = {
    "status": 404,
    "content": {
        "message": "Not Found"
    }
}

# Connection info
DB_NAME = os.environ['OU_DBNAME']
HOST = os.environ['OU_HOST']
PORT = os.environ['OU_PORT']
USER = os.environ['OU_USER']
DB_PASSWORD = os.environ['OU_PASSWORD']

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

        price_key = city + item
        price = price_cache.get(price_key)
        if price is None:
            item_count, mode = self.query_item_price_db(city, item)
            price = {
                "status": 200,
                "content": {
                    "item": "Furniture",
                    "item_count": item_count,
                    "price_suggestion": mode,
                    "city": "Philadelphia"
                }
            }
            price_cache.set(price_key, price, timeout=5 * 60)
        return price

    def query_item_price_db(self, city, item):
        connection = psycopg2.connect(dbname=DB_NAME, password=DB_PASSWORD, host=HOST, user=USER, port=PORT)
        cursor = connection.cursor()
        if city and item:
            city_clause = " AND city='{city}'".format(city=city)
        else:
            city_clause = ""
        count_query_string = '\
            SELECT COUNT(sell_price)\
            FROM "itemPrices_itemsale"\
            WHERE title=\'{item}\'\
            {city_clause};\
        '.format(item=item, city_clause=city_clause)
        mode_query_string = '''
            SELECT sell_price, COUNT(sell_price) AS frequency
            FROM "itemPrices_itemsale"
            WHERE title=\'{item}\'
            {city_clause}
            GROUP BY sell_price
            ORDER BY COUNT(sell_price) DESC, sell_price DESC
            LIMIT 1
        '''.format(item=item, city_clause=city_clause)
        cursor.execute(count_query_string)
        count_results = cursor.fetchone()
        count = count_results[0]
        if count == 0:
            # No point in running mode query
            return None, None
        cursor.execute(mode_query_string)
        mode_results = cursor.fetchone()
        mode = mode_results[0]
        return count, mode


'''
id (int): unique id for an item, created by Postgres

title (char): title of the item (e.g. Xbox One)

list_price (int): the price at which the item was listed

sell_price (int): the price at which the item was sold

city (char): the city in which the item was listed

cashless (bool): true / false the seller will accept a credit card payment
'''



app = Flask(__name__)
api = Api(app)
api.add_resource(ItemPriceService, '/item_price_service')

if __name__ == '__main__':
    app.run(debug=True)