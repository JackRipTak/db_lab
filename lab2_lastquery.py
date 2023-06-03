import mysql.connector

config = {
    'user': 'root',
    'password': '12345678',
    'host': 'localhost',
    'database': 'db_test',
    'raise_on_warnings': True
}

conn = mysql.connector.connect(**config)
cursor = conn.cursor()

emp_nos = [10001, 10002, 10003]
dept_no_start = 1
dept_no_end = 9

for emp_no in emp_nos:
    dept_no = dept_no_start

    while dept_no <= dept_no_end:
        dept_no_formatted = f'd{dept_no:03}'

        sql_check = "SELECT COUNT(*) FROM dept_emp WHERE emp_no = %s AND dept_no = %s"
        values_check = (emp_no, dept_no_formatted)
        cursor.execute(sql_check, values_check)
        result = cursor.fetchone()

        if result[0] == 0:
            sql_insert = "INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date) VALUES (%s, %s, '2021-01-01', '2022-01-01')"
            values_insert = (emp_no, dept_no_formatted)
            cursor.execute(sql_insert, values_insert)

        dept_no += 1

conn.commit()
conn.close()