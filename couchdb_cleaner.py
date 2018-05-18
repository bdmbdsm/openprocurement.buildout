import ast
import sys
from openprocurement.api.utils import read_yaml
from couchdb.client import Server
from couchdb.http import Unauthorized


APP_META = './etc/app_meta.yaml'
config = read_yaml(APP_META)['config']

database_server = config['database']['couchdb.url']
database_name = config['database']['couchdb.db_name']


def base_view(items):
    """Returns a javascript function for filtering records in database
    :param items: An string representation of a logic statement in javascript
    :return: An string representation of javascript function
    """
    return '''function(doc) {
         if(%s) {
            emit(doc);
         }
     }
''' % items


def make_statement(field, value, sub_key=None, sub_query=False):
    """Returns string representation of a javascript object
       and his property, and checks if a property is equal to a value
    :param field: a property of a object
    :param value: value which will be checked
    :param sub_key: a sub property of a property
    :param sub_query: if True, return key, and a sub key of object
    :return: string representation of a javascript object and his keys

    Example without subquery
    >>> make_statement('_id', '12345')
    doc._id == '12345'

    Example with subquery
    >>> make_statement('dog', 'Marti', 'pets', sub_query=True)
    doc.pets.dog == 'Marti'
    """
    if sub_query:
        return "doc.{}.{} == '{}'".format(sub_key, field, value)
    return "doc.{} == '{}'".format(field, value)


def convert_to_dict(data):
    """Convert a string representation of dictionary
       into python dictionary type
    :param data: An string representation of a dictionary
    :return: dictionary object

    >>> convert_to_dict("{'_id': '1234'}")
    {'_id': '1234'}
    """
    return ast.literal_eval(data)


def has_sub_query(query):
    """Checks whether an keys, has a subquery
    :param query: Checks whether an value of a key is a dictionary
    :return: if value is a dictionary, returns True otherwise False
     >>> my_dict = {'key': {'martin': True}}
     >>> has_sub_query(my_dict)
     True
    """
    if isinstance(query, dict):
        return True
    return False


def make_query(parameters):
    """Build a main query for CouchDB
    :param parameters: A dictionary object
    :return: An string representation of logical statement in javascript

    >>> make_query({'doc_type': 'Lot', 'lotType' : {'procurement': 'ok', 'status': '200'}})
    (doc.lotType.status == '200' && doc.lotType.procurement == 'ok' && doc.doc_type == 'Lot')
    """
    sub_query = []
    for key in parameters.keys():
        if has_sub_query(parameters[key]):
            for k, v in parameters[key].items():
                sub_query.append(make_statement(k, v, key, True))
            parameters.pop(key)

    prepare_query = [make_statement(k, v) for k, v in parameters.items()]
    output = sub_query + prepare_query

    if len(output) == 1:
        return base_view(''.join(output))
    return base_view(' && '.join(output))


def get_metadata(obj):
    """Returns object's ID and REV keys
    :param obj: A CoachDB object
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
    """Delete records from CoachDB
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
    limit_query = raw_input('Enter a query limit: ').strip() or 100
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
