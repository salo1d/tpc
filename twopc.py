import psycopg2
import json
import pandas as pd

INIT_STATE = 'INIT'
PRECOMMIT_STATE = 'PRE_COMMIT'
COMMIT_STATE = 'COMMIT'
ABORT_STATE = 'ABORT'

class TM: #transaction manager
    def __init__(self, file):
        self.__auth_file = file
        self.__state = INIT_STATE
        self.__conn = None
        self.__database = None
        self.__table = None
        self.__schema = None
    
    def get_state(self):
        return self.__state
    
    def auth(self):
        try:
            with open(self.__auth_file) as file:
                creds = json.load(file)
        except:
            print('Something going wrong with creds.')
        else:
            self.__database = creds['database']
        try:
            conn = psycopg2.connect(host = creds['host'], database = creds['database'], user = creds['user'], password = creds['password'])
        except:
            print('Error with connection.')
            return False
        else:
            return conn
    
    def set_conn(self):
        self.__conn = self.auth()
        if (self.__conn == 0):
            print('Can not set root connection.')
            return False
        else:
            return True
        
    def set_schema(self):
        conn = self.auth()
        if (conn == 0):
            print('Can not get schema name.')
            return False
        else:
            cursor = conn.cursor()
            sql = """select nspname from pg_catalog.pg_namespace where nspname not in ('pg_toast','pg_temp_1',
                        'pg_toast_temp_1',
                        'pg_catalog',
                        'public',
                        'information_schema');""";
            cursor.execute(sql);
            self.__schema = cursor.fetchall()[0][0]
            return True
        
    def set_table(self):
        conn = self.auth()
        if (conn == 0):
            print('Can not get table name.')
            return False
        else:
            cursor = conn.cursor()
            sql = "SELECT * FROM pg_catalog.pg_tables where schemaname = '{}';".format(self.__schema);
            cursor.execute(sql);
            self.__table = cursor.fetchall()[0][1]
            return True
    
    
    def get_table_content(self):
        conn = self.auth()
        if (conn == 0):
            print('Can not get content of table.')
            return False
        else:
            cursor = conn.cursor()
            sql = """
            select 
            *
            from {1}.{0}
            ;
            """.format(self.__table, self.__schema)
            cursor.execute(sql)
            data = cursor.fetchall()
            return pd.DataFrame(data = data)
    
    
    def show_prepared(self):
        conn = self.auth()
        if (conn == 0):
            print('Can not show prepared transactions.')
        else:
            cursor = conn.cursor()
            sql = """
            select 
            *
            from pg_prepared_xacts
            where 
            database = '{0}'
            ;
            """.format(self.__database)
            try:
                cursor.execute(sql)
            except:
                print('Error while showing prepared.')
            else:
                return cursor.fetchall()
    
    
    def prepare(self, sql):
        try:
            xid = self.__conn.xid(format_id = ord(self.__database[0]), gtrid = '{0}'.format(self.__database), bqual = 'xyz')
            self.__conn.tpc_begin(xid)
            self.__conn.cursor().execute(sql)
            self.__conn.tpc_prepare()
        except:
            self.__state = ABORT_STATE
            return False
        else:
            self.__state = PRECOMMIT_STATE
            return True
        
    
    def rollback_prepared(self):
            try:
                self.__conn.tpc_rollback()
            except:
                return False
            else:
                return True
        
    def commit_prepared(self):
        try:
            self.__conn.tpc_commit()
        except:
            return False
        else:
            return True
    
    def commit_or_rollback(self, message):
        if (message == 1):
             res = self.commit_prepared()
        else:
            res = self.rollback_prepared()
        if (res == 0):
            return False
        else:
            return True

class Coord_TM:
    def __init__(self):
        self.__state = INIT_STATE
        
    def get_state(self):
        return self.__state
    
    def sent_commit_message(self, name='Bob', fly_number='ASD432',
                            fly_from='ASD', fly_to='QWE', date='2021-09-10',
                            end_date='2021-09-11', hotel_name='InterContinental' ):
        sql_1 = '''
        insert into fly.fly_table ("name", "fly_number", "from", "to", "date") 
        values('{}', '{}', '{}', '{}', '{}');
        '''.format(name, fly_number, fly_from, fly_to, date)
        sql_2 = '''
        insert into hotel.hotel_table ("name", "hotel_name", "arrival", "departure") 
        values('{}', '{}', '{}', '{}');
        '''.format(name, hotel_name, date, end_date)
        sql_3 = '''
        update account.account_table set "amount" = 
        (select "amount" from account.account_table where "name"='{}') - 2
        where "name"='{}';
        '''.format(name, name)
        self.__state = PRECOMMIT_STATE
        return sql_1, sql_2, sql_3
    
    def get_votes_and_complete(self, vote1, vote2, vote3):
        if (vote1 == 1 and vote2 == 1 and vote3 == 1):
            self.__state = COMMIT_STATE
            return True
        else:
            self.__state = ABORT_STATE
            return False


user1 = TM("creds_fly.txt")
user1.set_conn()
user1.set_schema()
user1.set_table()
print(user1.get_state())
print(user1.get_table_content())

user2 = TM("creds_hotel.txt")
user2.set_conn()
user2.set_schema()
user2.set_table()
print(user2.get_state())
print(user2.get_table_content())

user3 = TM("creds_account.txt")
user3.set_conn()
user3.set_schema()
user3.set_table()
print(user3.get_state())
print(user3.get_table_content())

coord = Coord_TM()
print(coord.get_state())

buff = coord.sent_commit_message()
mess1 = buff[0]
mess2 = buff[1]
mess3 = buff[2]
print(mess1)
print(mess2)
print(mess3)

vote1 = user1.prepare(mess1)
print('user1')
print(vote1)
print(user1.show_prepared())
print(user1.get_state())

vote2 = user2.prepare(mess2)
print('user2')
print(vote2)
print(user2.show_prepared())
print(user2.get_state())

vote3 = user3.prepare(mess3)
print('user3')
print(vote3)
print(user3.show_prepared())
print(user3.get_state())

mess = coord.get_votes_and_complete(vote1, vote2, vote3)
print(mess)
print(coord.get_state())

user1.commit_or_rollback(mess)
print(user1.get_table_content())

user2.commit_or_rollback(mess)
print(user2.get_table_content())

# user3.commit_or_rollback(mess)
print(user3.get_table_content())