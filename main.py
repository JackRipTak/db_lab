import mysql.connector

# 配置数据库连接参数
config = {
    'user': 'root',
    'password': '12345678',
    'host': 'localhost',
    'database': 'db_test',
    'raise_on_warnings': True
}

# 连接到MySQL数据库
conn = mysql.connector.connect(**config)
cursor = conn.cursor()
'''
# 创建新表
cursor.execute(CREATE TABLE IF NOT EXISTS new_table (
                    emp_no INTEGER,
                    name TEXT,
                    dept_no INTEGER,
                    manager_no INTEGER
                    ))
'''
# 执行查询语句
select_query = '''
SELECT tmp1.emp_no, tmp1.name, tmp1.dept_no, min(dm.emp_no) as manager_no
FROM (
    SELECT e.emp_no, CONCAT(e.first_name, ' ', e.last_name) AS name, min(de.dept_no) as dept_no
    FROM employees e
    LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
    GROUP BY e.emp_no, CONCAT(e.first_name, ' ', e.last_name)
    HAVING COUNT(DISTINCT de.dept_no) >= 2
) as tmp1
LEFT JOIN dept_manager dm ON tmp1.dept_no = dm.dept_no
GROUP BY tmp1.emp_no, tmp1.dept_no
'''

cursor.execute(select_query)

# 获取查询结果
results = cursor.fetchall()

# 将结果插入新表
insert_query = '''INSERT INTO multidept_emp (emp_no, name, dept_no, manager_no)
                  VALUES (%s, %s, %s, %s)'''

for row in results:
    cursor.execute(insert_query, row)

# 提交更改并关闭连接
conn.commit()
conn.close()
