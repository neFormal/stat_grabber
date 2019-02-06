#!/usr/bin/env python3

import re
import psycopg2
import json
from io import StringIO

print('test passed')
print(__name__)

#  id | user_id |      name       | count 
# ----+---------+-----------------+-------
#   1 |       1 | badge_shen      |     5 
#   2 |       1 | badge_meklar    |     5 
#   3 |       1 | badge_mara      |     5 
#   4 |       1 | badge_jack      |     5 
#   5 |       1 | badge_universal |     5 
#   6 |       1 | credits         |  1000

regexp_string = "^(\\S+) \\[([\\w:\\-\\+]+)\\] \"(\\S+)\""
regex = re.compile(regexp_string)

ss = """
127.0.0.1 [2018-06-12T22:24:40+03:00] "foo=bar&user_id=1&level=1"
127.0.0.1 [2018-06-12T22:25:35+03:00] "msg=test&user_id=1&level=1&id=2&a1=1&a2=2&a3=3"
127.0.0.1 [2018-06-12T22:25:43+03:00] "msg=test&user_id=1&level=1&id=3&a1=5&a22=2&a3=83"
"""

def insert():
    xs = '\n'.join([f"{x}\t{x*10}\tcopy_insert\t{x}" for x in range(10, 20)])
    f = StringIO(xs)
    # print(f.readlines())

    connect_str = "dbname='test' user='user' host='localhost' password='password'"
    conn = psycopg2.connect(connect_str)
    print('conn: ', conn)
    cur = conn.cursor()
    print('cur: ', cur)
    
    cur.copy_from(f, 'items', columns=('id', 'user_id', 'name', 'count'))
    cur.execute("select * from items;")
    result = cur.fetchall()
    cur.close()
    cur = None
    conn.commit()
    conn.close()
    conn = None
    print(result)


def make_row(parsed):
    if not parsed:
        return None
    
    ip, ts, params = parsed[0]
    d = dict(map(lambda x: x.split('='), params.split('&')))
    if 'msg' not in d:
        return None

    # print('row:', '%s\t%s\t%s\t%s\t%s'%(ts, ip, d.get('user_id', 1), d.get('level', 1), json.dumps(d)))
    print('row:', f"{ts}\t{ip}\t{d.get('user_id', 1)}\t{d.get('level', 1)}\t{json.dumps(d)}")
    # return '%s\t%s\t%s\t%s\t%s'%(ts, ip, d.get('user_id', 1), d.get('level', 1), json.dumps(d))
    return f"{ts}\t{ip}\t{d.get('user_id', 1)}\t{d.get('level', 1)}\t{json.dumps(d)}"
    # return "2018-06-12T22:24:40+03:00/127.0.2.1/2/3/{}"


def insert2(table, columns, data):
    try:
        ##
        connect_str = "dbname='test2' user='user' host='localhost' password='password'"
        conn = psycopg2.connect(connect_str)
        print('conn: ', conn)
        cur = conn.cursor()
        print('cur: ', cur)

        ##
        # for x in [x for x in data.split('\n') if x != '']:
        #     print('x:', regex.findall(x))
        # print('rows:', '\n'.join(filter(lambda row: row is not None, [make_row(regex.findall(x)) for x in data.split('\n') if x != ''] )))
        rows = StringIO('\n'.join(filter(lambda row: row is not None, [make_row(regex.findall(x)) for x in data.split('\n') if x != ''] )))
        # for x in rows:
        #     yield x
        
        ##
        cur.copy_from(rows, table, columns=columns, sep="\t")
        cur.execute("select * from auth;")
        
        cur.close()
        conn.commit()
        conn.close()
        cur = None
        
        pass
    except Exception as e:
        print('Exception:', e)

def read():
    # for x in ss.split('\n'):
    #     yield regex.findall(x)

    for x in insert2('auth', ('datetime', 'ip', 'user_id', 'level', 'params'), ss):
        yield x

if __name__ == '__main__':
    print('run')
    print([x for x in read()])
    # insert()




