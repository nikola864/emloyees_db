import psycopg2
from psycopg2 import sql
from seaborn.external.docscrape import header
from tabulate import tabulate
import click
from datetime import datetime

DB_CONFIG = {
    "dbname": "employees_db",
    'user':'postgres',
    'password': 'kolya2468',
    'host': 'localhost'
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

@click.group()
def cli():
    """Консольное приложение для управления базой данных сотрудников"""
    pass
@cli.command()
@click.option('--full_name', prompt='ФИО', help='Полное имя сотрудника')
@click.option('--position', prompt='должность', help='Должность сотрудника')
@click.option('--hire_date', prompt='Дата приема (YYYY-MM-DD)', help='Дата приема на работу')
@click.option('--salary', prompt='зарплата', type=float, help='Размер заработной платы')
@click.option('--manager_id', type=int, help='ID начальника')
def add(full_name, position, hire_date, salary, manager_id):
    """Добавить нового сотрудника"""
    try:
        hire_date = datetime.strptime(hire_date, '%Y-%m-%d').date()

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'INSERT INTO employees (full_name, position, hire_date, salary, manager_id)'
                    'VALUES (%s, %s, %s, %s, %s) RETURNING id',
                    (full_name, position, hire_date, salary, manager_id)
                )

                employee_id = cursor.fetchone()[0]
                click.echo(f'Cотрудник добавлен с ID: {employee_id}')
    except Exception as e:
        click.echo(f'Ошибка: {e}')

@cli.command()
@click.argument('employee_id', type=int)
def get(employee_id):
    """Получить информацию о сотруднике по ID"""
    try:
        with get_connection() as conn:

            with conn.cursor() as cursor:
    # Запрос с JOIN для получения имени начальника
                cursor.execute(
                    'SELECT e.id, e.full_name, e.position, e.hire_date, e.salary,'
                    'm.full_name as manager_name'
                    'FROM employees e LEFT JOIN employees m ON e.manager_id=m.id'
                    'WHERE e.id=%s',
                    (employee_id,)
                )
                employee = cursor.fetchone()
                if employee:
                    headers=['ID','ФИО','Должность','Дата приёма','Зарплата','Начальник']
                    click.echo(tabulate([employee], headers=headers, tablefmt='grid'))
                else:
                    click.echo('Сотрудник не найден')
    except Exception as e:
        click.echo(f'Ошибка: {e}')

@cli.command()
@click.option('--position', help='Фильтр по должности')
@click.option('--min_salary', type=float, help='Минимальная зарплата')
@click.option('--max_salary', type=float, help='Максимальная зарплата')
@click.option('--hire_date_from', help='Дата приема от (YYYY-MM-DD)')
@click.option('--hire_date_to', help='Дата приема до (YYYY-MM-DD)')
@click.option('--sort', help='Поле для сортировки (name, position, salary, hire_date)')
@click.option('--limit', type=int, default=100, help='Лимит записей')
def list(position, min_salary, max_salary, hire_date_from, hire_date_to, sort, limit):
    """Список сотрудников с фильтрами и сортировкой"""
    try:
        query='''
        SELECT e.id, e.full_name, e.position, e.hire_date, e.salary,
        m.full_name as manager_name
        FROM employees e LEFT JOIN employees m ON e.manager_id = m.id
        '''
        params = []
        conditions = []

        if position:
            conditions.append('e.position = %s')
            params.append(position)
        if min_salary is not None:
            conditions.append('e.salary >= %s')
            params.append(min_salary)
        if max_salary is not None:
            conditions.append('e.salary <= %s')
            params.append(max_salary)
        if hire_date_from:
            conditions.append('e.hire_date >= %s')
            params.append(datetime.strptime(hire_date_from, '%Y-%m-%d').date())
        if hire_date_to:
            conditions.append('e.hire_date >= %s')
            params.append(datetime.strptime(hire_date_from, '%Y-%m-%d').date())

        if conditions:
            query += 'WHERE ' + 'AND'.join(conditions)

        if sort:
            sort_mapping = {
                'name': 'e.full_name',
                'position': 'e.position',
                'salary': 'e.salary',
                'hire_date': 'e.hire_date'
            }
            sort_field = sort_mapping.get(sort, 'e.id')
            query += f' ORDER BY {sort_field}'

        query += ' LIMIT %s'
        params.append(limit)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                employees = cursor.fetchone()
                if employees:
                    headers = ['ID','ФИО','Должность', 'Дата приёма', 'Зарплата', 'Начальник']
                    click.echo(tabulate(employees, headers=headers, tablefmt='grid'))
                else:
                    click.echo('Сотрудники не найдены')
    except Exception as e:
        click.echo(f'Ошибка: {e}')

@cli.command()
@click.argument('employee_id', type=int)
@click.option('--full_name', help='Новое ФИО')
@click.option('--position', help='Новая должность')
@click.option('--hire_date', help='Новая дата приема (YYYY-MM-DD)')
@click.option('--salary', type=float, help='Новая зарплата')
@click.option('--manager_id', type=int, help='Новый ID начальника')
def update(employee_id, full_name, position, hire_date, salary, manager_id):
    """Обновить данные сотрудника"""
    try:
        updates = []
        params = []

        if full_name:
            updates.append('full_name = %s')
            params.append(full_name)
        if position:
            updates.append('position= %s')
            params.append(position)
        if hire_date:
            updates.append("hire_date = %s")
            params.append(datetime.strptime(hire_date, '%Y-%m-%d').date())
        if salary:
            updates.append("salary = %s")
            params.append(salary)
        if manager_id:
            updates.append("manager_id = %s")
            params.append(manager_id)

        if not updates:
            click.echo('Не указаны данные для обновления')
            return
        params.append(employee_id)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f'UPDATE employees SET {', '.join(updates)} WHERE id = %s',
                    params
                )
                if cursor.rowcount > 0:
                    click.echo(f"Данные сотрудника с ID {employee_id} обновлены")
                else:
                    click.echo('Сотрудник не найден')
    except Exception as e:
        click.echo(f"Ошибка: {e}")

@cli.command()
@click.argument('employee_id', type=int)
def delete(employee_id):
    """Удалить сотрудника по ID"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT COUNT(*) FROM employees WHERE manager_id = %s',
                    (employee_id,)
                )
                subordinates_count = cursor.fetchone()[0]

    # Если есть подчиненные - отмена удаления
                if subordinates_count > 0:
                    click.echo(f"Ошибка: у сотрудника есть {subordinates_count} подчиненных. "
                            "Сначала переназначьте их или удалите.")
                    return

                cursor.execute(
                    'DELETE FROM employees WHERE id = %s',
                    (employee_id,)
                )
                if cursor.rowcount > 0:
                    click.echo(f"Сотрудник с ID {employee_id} удален")
                else:
                    click.echo('Сотрудник не найден')
    except Exception as e:
        click.echo(f"Ошибка: {e}")

@cli.command()
def stats():
    """Статистика по сотрудникам"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:

                cursor.execute('SELECT COUNT(*) FROM employees')
                total = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT position) FROM employees")
                positions = cursor.fetchone()[0]

                cursor.execute("SELECT MIN(hire_date), MAX(hire_date) FROM employees")
                min_date, max_date = cursor.fetchone()

                cursor.execute("SELECT AVG(salary), MIN(salary), MAX(salary) FROM employees")
                avg_salary, min_salary, max_salary = cursor.fetchone()

                click.echo('Общая статистика:')
                click.echo(f'Всего сотрудников: {total}')
                click.echo(f"Количество должностей: {positions}")
                click.echo(f"Даты приема: с {min_date} по {max_date}")
                click.echo(f"Зарплаты: средняя {avg_salary:.2f}, min {min_salary}, max {max_salary}")

                cursor.execute('''
                    SELECT position, count(*) as count,
                        avg(salary) as avg_salary,
                        min(salary) as min_salary,
                        max(salary) as max_salary
                    FROM employees
                    GROUP BY position
                    ORDER BY avg_salary DESC
                ''')
                position_stats = cursor.fetchall()

                click.echo("\nСтатистика по должностям:")
                headers = ['Должность', 'Количество', 'Ср. зарплата', 'Мин.', 'Макс.']
                click.echo(tabulate(position_stats, headers=headers, tablefmt='grid'))

    except Exception as e:
        click.echo(f"Ошибка: {e}")

if __name__ == '__main__':
    cli()
























