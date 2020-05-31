# -*- coding: utf-8 -*-
import json
import psycopg2
from sys import stdin
from sys import exit
# employee(id,supervisor_id,password, data, time_in, time_out) 6 values

err = "{\"status\": \"ERROR\"}"
ok = "{\"status\": \"OK\"}"


def child(employee_id):
    try:
        cur.execute("select * from employee where id = %s;", (employee_id,))
        if cur.fetchone() is None:
            print err
            return
        cur.execute("select id from employee where supervisor_id = %s;", (employee_id,))
        ids = cur.fetchall()
        data = [str(id_[0]) for id_ in ids]
        col = ('status', 'data')
        #print d
        #print "{\"status\": \"OK\"}", data
        print json.dumps(dict(zip(col, ['OK', data])))


    except psycopg2.Error as e:
        conn.rollback()
        print err


def descendants(employee_id):
    try:
        cur.execute("select time_in, time_out from employee where id = %s;", (employee_id,))
        time_io = cur.fetchone()
        if time_io is None: # nie ma takiego id
            print "{\"status\": \"ERROR\",\"debug\":\"failed descendants, no id\"}"
            return

        cur.execute("select id from employee where time_in > %s and time_out < %s order by id asc;", (time_io[0], time_io[1]))
        ids = cur.fetchall()
        data = [str(id_[0]) for id_ in ids]
        #print "{\"status\": \"OK\"}", data
        print json.dumps(dict(zip(['status', 'data'], ['OK', data])))
    except psycopg2.Error as e:
        conn.rollback()
        print "{\"status\": \"ERROR\",\"debug\":\"failed descendants\"}"


def ancestors(employee_id):
    try:
        cur.execute("select time_in, time_out from employee where id = %s;", (employee_id,))
        times_emp = cur.fetchone()
        if times_emp is None:
            print err
        else:
            cur.execute("select id from employee where time_in < %s and time_out > %s order by id asc;", (times_emp[0], times_emp[1]))
            ids = cur.fetchall()
            data = [str(id_[0]) for id_ in ids]
            print json.dumps(dict(zip(['status', 'data'], ['OK', data])))
    except psycopg2.Error as e:
        conn.rollback()
        print err


def ancestor(supervisor_id, subordinate_id, pr = True):
    try:
        cur.execute("select time_in, time_out from employee where id = %s;", (supervisor_id,))
        times_sup = cur.fetchone()
        cur.execute("select time_in, time_out from employee where id = %s;", (subordinate_id,))
        times_sub = cur.fetchone()

        if times_sub is None or times_sup is None:
            if pr: print "{\"status\": \"ERROR\",\"debug\":\"failed ancestor\"}"
            return False

        else:
            if times_sup[0] < times_sub[0] and times_sup[1] > times_sub[1]:
                if pr: print json.dumps(dict(zip(['status', 'data'], ['OK', ['true']])))
                return True
            else:
                if pr: print json.dumps(dict(zip(['status', 'data'], ['OK', ['false']])))
                return False

    except psycopg2.Error as e:
        conn.rollback()
        print err


def remove(admin_id, employee_id):
    #cur.execute("select time_in, time_out from employee where id in (%s, %s);", (admin_id, employee_id))
    #times = cur.fetchall()
    try:
        cur.execute("select time_in, time_out from employee where id = %s;", (admin_id,))
        times_sup = cur.fetchone()
        cur.execute("select time_in, time_out from employee where id = %s;", (employee_id,))
        times_sub = cur.fetchone()
        if times_sub is None or times_sup is None:
            print "{\"status\": \"ERROR\",\"debug\":\"failed remove\"}"
            return False


        if times_sup[0] < times_sub[0] and times_sup[1] > times_sub[1]:
            sub = times_sub[1] - times_sub[0] + 1
            cur.execute("delete from employee where time_in >= %s and time_out <= %s;", (times_sub[0], times_sub[1]))
            cur.execute("update employee set time_in = time_in - %s where time_in > %s;", (sub, times_sub[1]))
            cur.execute("update employee set time_out = time_out - %s where time_out > %s;", (sub, times_sub[1]))
            conn.commit()
            print "{\"status\": \"OK\"}"
        else:
            print "{\"status\": \"ERROR\",\"debug\":\"failed remove\"}"

    except psycopg2.Error as e:
        conn.rollback()
        print err


def root(password, data, r_id):
    try:
        cur.execute("insert into employee values(%s, NULL, %s, %s, 1, 2);", (r_id, password, data))
        conn.commit()
        print "{\"status\": \"OK\"}"
    except psycopg2.Error as e:
        conn.rollback()
        print err


def new(admin_id, employee_id, parent_id, data, password, new_init=0):
    try:

        '''sprawdzenie czy admin jest przełożonym parent_id, jeśli nowy pracownik jest dodawany przez app '''
        if new_init == 0:
            if not (ancestor(admin_id, parent_id, False) or admin_id == parent_id):
                print "{\"status\": \"ERROR\"}"
                return

        cur.execute("select time_in, time_out from employee where id = %s;", (parent_id,))
        time_io = cur.fetchone()

        ''' przełożony nie istnieje '''
        if time_io is None:
            print err
            return

        ''' id ostatnio dodanego podwładnego '''
        cur.execute("select id from employee where time_out = %s - 1", (time_io[1],))
        last_sub = cur.fetchone()


        if last_sub is None:
            t = time_io[0]
            cur.execute("update employee set time_out = time_out + 2 where time_out > %s;", (t,))
            cur.execute("update employee set time_in = time_in + 2 where time_in > %s;", (t,))

            cur.execute("insert into employee \
                             values(%s, %s, %s, %s, %s, %s);", (employee_id, parent_id, password, data, t+1, t+2))
            conn.commit()
            print "{\"status\": \"OK\"}"

        else:
            # cur.execute("select time_out from employee where id = %s", (last_sub[0],))
            # t = cur.fetchone()[0]
            t = time_io[1] - 1
            cur.execute("update employee set time_out = time_out + 2 where time_out > %s;", (t,))
            cur.execute("update employee set time_in = time_in + 2 where time_in > %s;", (t,))

            cur.execute("insert into employee \
                         values(%s, %s, %s, %s, %s, %s);", (employee_id, parent_id, password, data, t+1, t+2))
            conn.commit()
            print "{\"status\": \"OK\"}"

    except psycopg2.Error as e:
        conn.rollback()
        print err


def parent(child_id):
    try:
        cur.execute("select supervisor_id from employee where id = %s;", (child_id,))
        p_id = cur.fetchone()
        if p_id is None:
            print "{\"status\": \"ERROR\"}"
        else:
            print json.dumps(dict(zip(['status', 'data'], ['OK', [str(p_id[0])]])))
    except psycopg2.Error as e:
        conn.rollback()
        print err


def read(admin_id, employee_id):
    if admin_id == employee_id or ancestor(admin_id, employee_id, False):
        try:
            cur.execute("select data from employee where id = %s;", (employee_id,))
            d = cur.fetchone()
            if d is None:
                print "{\"status\": \"ERROR\"}"
            else:
                print json.dumps(dict(zip(['status', 'data'], ['OK', [d[0]]])))
        except psycopg2.Error as e:
           conn.rollback()
           print err
    else:
        print "{\"status\": \"ERROR\"}"


def update(admin_id, employee_id, new_data):
    if admin_id == employee_id or ancestor(admin_id, employee_id, False):
        try:
            cur.execute("update employee set data = %s where id = %s;", (new_data, employee_id))
            print ok
        except psycopg2.Error as e:
           conn.rollback()
           print err

    else:
        print "{\"status\": \"ERROR\"}"

#MAIN
opendb = json.loads(stdin.readline())

try:
    conn = psycopg2.connect(dbname = opendb["open"]["database"], user = opendb["open"]["login"], password = opendb["open"]["password"])
    print ok
except:
    print err
    exit(1)

cur = conn.cursor()

if opendb["open"]["login"] == "init":
    ''' wczytanie modelu fizycznego '''
    try:
        cur.execute(open("baza.sql","r").read())
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print err

    line = json.loads(stdin.readline())
    if line["root"]["secret"] == "qwerty":
        root(line["root"]["newpassword"], line["root"]["data"], line["root"]["emp"])
    else:
        print err

    for li in stdin.readlines():
        command = json.loads(li)
        func_name = command.keys()[0]
        command_1 = command[func_name]
        cur.execute("select password from employee where id = %s", (command_1["admin"],))

        if cur.fetchone()[0] == command_1["passwd"]:
            new(command_1["admin"], command_1["emp"], command_1["emp1"], command_1["data"], command_1["newpasswd"], 1)
        else:
            print("{\"status\": \"ERROR\", \"debug\":\"incorrect password\"}")
    conn.commit()


elif opendb["open"]["login"] == "app":
    for line in stdin.readlines():
        command = json.loads(line)
        func_name = command.keys()[0]
        command_1 = command[func_name]
        cur.execute("select password from employee where id = %s;", (command_1["admin"],))
        pswd = cur.fetchone()

        ''' admin nie istnieje '''
        if pswd is None:
            print err

        else:
            if pswd[0] == command_1["passwd"]:
                if func_name == "new":
                    new(command_1["admin"], command_1["emp"], command_1["emp1"], command_1["data"], command_1["newpasswd"])
                
                elif func_name == "parent":
                    parent(command_1["emp"])

                elif func_name == "remove":
                    remove(command_1["admin"], command_1["emp"])

                elif func_name == "descendants":
                    descendants(command_1["emp"])

                elif func_name == "ancestors":
                    ancestors(command_1["emp"])

                elif func_name == "ancestor":
                    ancestor(command_1["emp2"], command_1["emp1"])

                elif func_name == "child":
                    child(command_1["emp"])

                elif func_name == "read":
                    read(command_1["admin"],command_1["emp"])

                elif func_name == "remove":
                    remove(command_1["admin"], command_1["emp"])

                elif func_name == "update":
                    update(command_1["admin"], command_1["emp"], command_1["newdata"])
            else:
                print("{\"status\": \"ERROR\", \"debug\":\"incorrect password\"}")

else:
    print err
# print "\n"
# cur.execute("select * from employee;")
# print cur.fetchall()

cur.close()
conn.close()
