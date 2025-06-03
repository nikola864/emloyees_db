import random
from datetime import datetime, timedelta
from mimesis import Person, Address, Datetime
from mimesis.enums import Gender
import psycopg2
from psycopg2 import sql

# Подключение к БД
conn = psycopg2.connect(
    dbname="employees_db",
    user="postgres",
    password="kolya2468",
    host="localhost",
    client_encoding='UTF8'
)
cursor = conn.cursor()

# создание генераторов
person = Person('ru')
address = Address('ru')
dt = Datetime('ru')

# уровни иерархии с примерным распределением
positions_hierarchy = {  # Переименовали переменную
    'CEO': 1,
    'Director': 10,
    'Manager': 100,
    'Team lead': 1000,
    'Senior Developer': 5000,
    'Developer': 44000
}

def generate_employees():
    employees = []
    id_counter = 1

    # генерация CEO (у него нет начальника)
    ceo = {
        'id': id_counter,
        'full_name': person.full_name(gender=Gender.MALE),
        'position': 'CEO',
        'hire_date': dt.date(start=2000, end=2005),
        'salary': random.randint(300000, 500000),
        'manager_id': None
    }
    employees.append(ceo)
    id_counter += 1

    # Генерация остальных сотрудников
    for position_name, count in positions_hierarchy.items():  # Используем новое имя
        if position_name == 'CEO':
            continue

        for _ in range(count):
            # Выбираем случайного начальника из более высоких уровней
            if position_name == 'Director':
                manager_position = 'CEO'
            elif position_name == 'Manager':
                manager_position = random.choice(['CEO', 'Director'])
            elif position_name == 'Team lead':
                manager_position = random.choice(['Director', 'Manager'])
            elif position_name == 'Senior Developer':
                manager_position = random.choice(['Manager', 'Team lead'])
            elif position_name == 'Developer':
                manager_position = random.choice(['Team lead', 'Senior Developer'])

            # находим айди случайного начальника
            cursor.execute(
                "SELECT id FROM employees WHERE position = %s ORDER BY RANDOM() LIMIT 1",
                (manager_position,)
            )
            result = cursor.fetchone()
            manager_id = result[0] if result else None

            # Определяем зарплату
            if position_name == 'Director':
                salary = random.randint(200000, 300000)
            elif position_name == 'Manager':
                salary = random.randint(150000, 200000)
            elif position_name == 'Team lead':
                salary = random.randint(100000, 150000)
            elif position_name == 'Senior Developer':
                salary = random.randint(80000, 100000)
            elif position_name == 'Developer':
                salary = random.randint(50000, 80000)

            employee = {
                'id': id_counter,
                'full_name': person.full_name(),
                'position': position_name,  # Используем position_name
                'hire_date': dt.date(start=2005, end=2023),
                'salary': salary,
                'manager_id': manager_id
            }
            employees.append(employee)
            id_counter += 1

            if len(employees) % 1000 == 0:
                insert_employees(employees)
                employees = []
                print(f"Generated {id_counter} employees")

    if employees:
        insert_employees(employees)

def insert_employees(employees):
    insert_query = sql.SQL(""" 
    INSERT INTO employees (id, full_name, position, hire_date, salary, manager_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    """)

    for emp in employees:
        cursor.execute(insert_query, (
            emp['id'],
            emp['full_name'],
            emp['position'],
            emp['hire_date'],
            emp['salary'],
            emp['manager_id']
        ))
    conn.commit()

if __name__ == '__main__':
    print('Generating employees...')
    generate_employees()
    print('Data generation completed!')
    cursor.close()
    conn.close()