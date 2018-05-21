import ast
import sys
import collections
from openprocurement.api.utils import read_yaml
from couchdb.client import Server
from couchdb.http import Unauthorized


APP_META = './etc/app_meta.yaml'
config = read_yaml(APP_META)['config']

database_server = config['database']['couchdb.url']
database_name = config['database']['couchdb.db_name']


def base_view(item):
    """Returns a javascript function for filtering records in database
    :param item: An string representation of a logic statement in javascript
    :return: An string representation of javascript function
    """
    return 'function(doc) {if(%s) {emit(doc);}}' % item


def make_keys(parameters, parent_key='doc', sep='.'):
    """Returns a dictionary with all nested keys
       And their values

    :param parameters: A dictionary with information
    :param parent_key: A string which you want to add to a key
    :param sep: A separator, which will match an keys
    :return: An dictionary, with keys or sub keys and their an values

    >>> data = {'lotType': {'type': {'commercial': {'shop': 'yes'}}}}
    >>> make_keys(data)
    {'doc.lotType.type.commercial.shop': 'yes'}
    """
    items = []
    for k, v in parameters.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(make_keys(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def make_query(query):
    """Returns string representation of a query for CouchDB
    :param query: A dictionary, with a information
    :return: A string representation of a javascript logic statement

    >>> data = {'lotType': {'type': {'commercial': {'shop': 'yes'}}}}
    >>> make_query(data)
    function(doc) {if(doc.doc.lotType.type.commercial.shop == 'yes') {emit(doc);}}
    """
    formatted_query = make_keys(query)
    prepare_query = ["{} == '{}'".format(key, value) for key, value in formatted_query.items()]
    output_query = base_view(' && '.join(prepare_query))
    return output_query


def convert_to_dict(data):
    """Convert a string representation of dictionary
       into python dictionary type
    :param data: An string representation of a dictionary
    :return: dictionary object

    >>> convert_to_dict("{'_id': '1234'}")
    {'_id': '1234'}
    """
    return ast.literal_eval(data)


def get_metadata(obj):
    """Returns object's ID and REV keys
    :param obj: A CouchDB object
    :return: a dictionary with object's information
    """
    return {'_id': obj['id'], '_rev': obj['key']['_rev']}


def welcome_message():
    """User friendly message"""
    print '=' * 35
    print 'Welcome in Database Cleaner\nPlease enter an credentials under'
    print '=' * 35


def make_database_connection(database_url):
    """Connects to a CouchDB database
    :param database_url: Database URL from app_meta.yaml file
    :return: an instance of database

    >>> make_database_connection('http://localhost:5984')
    """
    try:
        server = Server(database_url)
        return server[database_name]
    except Unauthorized:
        print 'ERROR: username or password is incorrect, try again.'
        sys.exit(1)


def make_database_url(username, password):
    """Make a full database url, with an user credentials
    :param username: A username of an admin
    :param password: A password of an admin
    :return: Database url

    >>> make_database_url('rabbit', 'marti', 'localhost:5984')
    'http://rabbit:marti@localhost:5984'
    """
    """Make a database url, with an user credentials"""
    host = database_server.split('@')[1]
    return 'http://{}:{}@{}'.format(username, password, host)


def delete(db, records):
    """Delete records from CouchDB
    :param db: an instance of database
    :param records: an array with an records meta information

    >>> delete(db, results)
    5 records has been successfully deleted.
    """
    deleted_items = []

    if not records:
        print 'There is no elements with such values in attributes'
        return False

    for record in records:
        deleted_items.append(get_metadata(record))

    db.purge(deleted_items)
    return True


def main():
    """Entry point"""
    welcome_message()
    username = raw_input('Enter your admin username:').strip()
    password = raw_input('Enter your password:').strip()
    db = make_database_connection(make_database_url(username, password))
    limit_query = raw_input('Enter a query limit(default is 100): ').strip() or 100
    search_query = raw_input('Enter your query: ').strip()

    generated_query = make_query(convert_to_dict(search_query))

    count = 0

    while True:
        result = db.query(generated_query, limit=int(limit_query))
        count += len(result)
        if not delete(db, result):
            print '{} items deleted'.format(count)
            break


if __name__ == '__main__':
    main()
