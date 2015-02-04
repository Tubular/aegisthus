import cqlsl
from cqlsl.sessions import Session
from cqlsl.statements import insert
from cassandra.cluster import Cluster
from datetime import datetime


cluster = Cluster(['192.168.200.10'])
session = Session(cluster, keyspace='test')

fields = ('bigint_field', 'boolean_field', 'float_field', 'int_field', 'list_int_field', 'set_text_field', 'map_time_field', 'map_long_field')
data = [
    (1, False, 1.32, 54, [1,2], {'a', 'b'}, {'first': datetime(2014, 1, 1, 2, 45), 'second': datetime(2015, 1, 2, 3)}, {'views': long(2.2 * 10 ** 9)}),
    (2, True, 0.4312, 22, None, {'c', 'd'}, {'first': datetime(2014, 2, 1, 2, 45)}, {'views': long(2.5 * 10 ** 9)}),
    (3, True, None, 5123, None, {'a', 'b', 'c'}, None, {'likes': 21}),
    (4, False, 23432.123, 5113, list(), set(), dict(), {'likes': 123123}),
    (5, None, 232.123, 5113, None, None, None, {'shares': 5 * 10 ** 9}),
]

for values in data:
    session.execute(insert('test_table').values(**dict(zip(fields, values))))
