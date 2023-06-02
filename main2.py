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

sql = "SELECT emp_no, first_name, last_name FROM employees"
cursor.execute(sql)
results = cursor.fetchall()
employees = [{'emp_no': row[0], 'first_name': row[1], 'last_name': row[2]} for row in results]

sql = "SELECT emp_no, dept_no FROM dept_emp"
cursor.execute(sql)
results = cursor.fetchall()
dept_emp = [{'emp_no': row[0], 'dept_no': row[1]} for row in results]

sql = "SELECT dept_no, emp_no FROM dept_manager"
cursor.execute(sql)
results = cursor.fetchall()
dept_manager = [{'dept_no': row[0], 'manager_no': row[1]} for row in results]

tmp1_dict = {}
for employee in employees:
    emp_no = employee['emp_no']
    name = employee['first_name'] + ' ' + employee['last_name']
    tmp1_dict[emp_no] = {'name': name, 'dept_no': None}

for relation in dept_emp:
    emp_no = relation['emp_no']
    dept_no = relation['dept_no']
    if tmp1_dict.get(emp_no) is not None:
        if tmp1_dict[emp_no]['dept_no'] is None:
            tmp1_dict[emp_no]['dept_no'] = dept_no
            tmp1_dict[emp_no]['is_multi'] = False
        else:
            tmp1_dict[emp_no]['is_multi'] = True

tmp1_filtered = [{'emp_no': emp_no, 'name': data['name'], 'dept_no': data['dept_no']} for emp_no, data in tmp1_dict.items() if data['is_multi']]
#tmp1_filtered = [row for row in tmp1_list if len([r for r in tmp1_list if r['emp_no'] == row['emp_no'] and r['dept_no'] != row['dept_no']]) >= 2]

results_dict = {}
for row in tmp1_filtered:
    emp_no = row['emp_no']
    dept_no = row['dept_no']
    if results_dict.get(emp_no) is None or results_dict[emp_no]['dept_no'] > dept_no:
        results_dict[emp_no] = {'name': row['name'], 'dept_no': dept_no, 'manager_no': None}

dept_manager_dict = {}
for manager in dept_manager:
    dept_no = manager['dept_no']
    manager_no = manager['manager_no']
    if dept_no in dept_manager_dict:
        if dept_manager_dict[dept_no] > manager_no:
            dept_manager_dict[dept_no] = manager_no
    else:
        dept_manager_dict[dept_no] = manager_no

for emp_data in tmp1_filtered:
    emp_no = emp_data['emp_no']
    dept_no = emp_data['dept_no']
    if dept_no in dept_manager_dict:
        manager_no = dept_manager_dict[dept_no]
        if emp_no in results_dict:
            if results_dict[emp_no]['manager_no'] is None or results_dict[emp_no]['manager_no'] > manager_no:
                results_dict[emp_no]['manager_no'] = manager_no


results_list = [{'emp_no': emp_no, 'name': data['name'], 'dept_no': data['dept_no'], 'manager_no': data['manager_no']} for emp_no, data in results_dict.items()]

with open('1.out', 'w') as file:
    for row in results_list:
        file.write(str(row)+'\n')

records = []
for emp_data in results_list:
    emp_no = emp_data['emp_no']
    name = emp_data['name']
    dept_no = emp_data['dept_no']
    manager_no = emp_data['manager_no']
    records.append((emp_no, name, dept_no, manager_no))

insert_query = '''
INSERT INTO multidept_emp (emp_no, name, dept_no, manager_no)
VALUES (%s, %s, %s, %s)
'''
cursor.executemany(insert_query, records)
conn.commit()

cursor.close()
conn.close()

'''

for relation in dept_emp:
    emp_no = relation['emp_no']
    dept_no = relation['dept_no']
    if tmp1_dict.get(emp_no) is not None:
        if tmp1_dict[emp_no]['dept_no'] is None or tmp1_dict[emp_no]['dept_no'] > dept_no:
            tmp1_dict[emp_no]['dept_no'] = dept_no

tmp1_list = [{'emp_no': emp_no, 'name': data['name'], 'dept_no': data['dept_no']} for emp_no, data in tmp1_dict.items()]
tmp1_filtered = [row for row in tmp1_list if len([r for r in tmp1_list if r['emp_no'] == row['emp_no'] and r['dept_no'] != row['dept_no']]) >= 2]

# 执行主查询
results_dict = {}
for row in tmp1_filtered:
    emp_no = row['emp_no']
    dept_no = row['dept_no']
    if results_dict.get(emp_no) is None or results_dict[emp_no]['dept_no'] > dept_no:
        results_dict[emp_no] = {'name': row['name'], 'dept_no': dept_no, 'manager_no': None}

for manager in dept_manager:
    dept_no = manager['dept_no']
    manager_no = manager['manager_no']
    if results_dict.get(dept_no) is not None:
        if results_dict[dept_no]['manager_no'] is None or results_dict[dept_no]['manager_no'] > manager_no:
            results_dict[dept_no]['manager_no'] = manager_no

results_list = [{'emp_no': emp_no, 'name': data['name'], 'dept_no': data['dept_no'], 'manager_no': data['manager_no']} for emp_no, data in results_dict.items()]

# 打印结果
for row in results_list:
    print(row)
'''