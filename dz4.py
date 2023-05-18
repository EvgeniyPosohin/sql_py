import psycopg2
import config
from pprint import pprint


conn = psycopg2.connect(database=config.database, user=config.user, password=config.password)


# создаем таблицы клиенты и телефоны
def creat_tb(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients (
	id_client SERIAL PRIMARY KEY,
	first_name VARCHAR(40),
	last_name varchar(40),
	email varchar(80) UNIQUE);
	""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phones (
	id_phone serial PRIMARY KEY,
	id_client integer,
	phone varchar(40),
	FOREIGN KEY (id_client) REFERENCES clients (id_client));
	""")
    return


# функция удаления
def delete_tb(cur):
    cur.execute("""
    DROP  TABLE clients, phones CASCADE;
    """)
    print('удалена')


# добавляем номера
def insert_phone(cur, id_с, phone):
    cur.execute("""
            INSERT INTO phones(id_client, phone)
            VALUES (%s, %s)""", (id_с, phone))


# добавляем номера клиентов, если есть номера, то добавляем их
def insert_client(cur, first_name=None, last_name=None, email=None, *phones):
    cur.execute("""
        INSERT INTO clients(first_name,	last_name,	email) 
        VALUES (%s, %s, %s)""", (first_name, last_name, email))
    cur.execute("""
        SELECT id_client FROM clients WHERE first_name = %s;
        """, (first_name,))
    id_client = cur.fetchone()[0]
    list_phone = list(phones)
    if not list_phone:
        print(f'Добавлен клиент ')
    else:
        for phone in list_phone:
            print(phone)
            insert_phone(cur, id_client, phone)
            print(f'Добавлен клиент')

# находим инфо клиента по id
def search_info(cur, id_c):
    cur.execute("""
        SELECT * from clients
        WHERE id_client = %s
        """, (id_c,))
    info = cur.fetchone()
    return info


# изменяем данные клиента
def update_client(cur, id_c, first_name=None, last_name=None, email=None):
    info = search_info(cur, id_c)
    if first_name is None:
        first_name = info[1]
    if last_name is None:
        last_name = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
        UPDATE clients
        SET first_name = %s, last_name = %s, email =%s 
        where id_client = %s
        """, (first_name, last_name, email, id_c))
    print(f'изменения внесены: {search_info(cur, id_c)}')


# удаляем номер
def delete_phone(cur, number):
    cur.execute("""
            SELECT * from phones
            WHERE phone = %s
            """, (number,))
    info = cur.fetchone()
    if not info:
        print('такой номер не найден')
    else:
        cur.execute("""
            DELETE FROM phones 
            WHERE phone = %s
            """, (number, ))
        print('номер удален')


# удаление клиента
def delete_client(cur, id_с):
    cur.execute("""
                SELECT * from phones
                WHERE id_client = %s
                """, (id_с,))
    info = cur.fetchone()
    if not info:
        print('ID не найден')
    else:
        cur.execute("""
            DELETE FROM phones
            WHERE id_client = %s
            """, (id_с, ))
        cur.execute("""
            DELETE FROM clients 
            WHERE id_client = %s
        """, (id_с,))
        print(f'удалены данные по id {id_с}')


# поиск клиента
def find_client(cur, cell):
    cur.execute("""
    SELECT  first_name, last_name, email, phone FROM clients c
    LEFT JOIN phones p ON p.id_client = c.id_client
    WHERE c.first_name=%s OR c.last_name=%s OR c.email=%s OR p.phone=%s;
    """, (str(cell), str(cell), str(cell), str(cell),))
    info = curs.fetchall()
    if not info:
        print('клиент не найден')
    else:
        print(info)


if __name__ == '__main__':
    with psycopg2.connect(database='dz-4', user='postgres', password='123456') as conn:
        with conn.cursor() as curs:
            delete_tb(curs)
            creat_tb(curs)
            # добавляем клиентов
            insert_client(curs, "Иван", "Иоанович", "masdds@gmail.com")
            insert_client(curs, "Василий", "Пупкин", "mpup@mail.ru", 895514785)
            insert_client(curs, "Евген", "Прошкин", "gjxnf@mail.com", 89632587411)
            insert_client(curs, "Александр", "Пушкин", "почты@нет.ru", 8952366447, 8566632, 5588445)
            insert_client(curs, "Анна", "Каренина", "ржд@привет.com")
            # получаем строки
            curs.execute("""
                            SELECT first_name, last_name, email, phone FROM clients c
                            LEFT JOIN phones p ON c.id_client = p.id_client
                            ORDER by c.id_client
                            """)
            pprint(curs.fetchall())
            # добавляем номера
            insert_phone(curs, 1, 12345678)
            insert_phone(curs, 5, 87654321)
            # получаем строки
            curs.execute("""
                                        SELECT first_name, last_name, email, phone FROM clients c
                                        LEFT JOIN phones p ON c.id_client = p.id_client
                                        ORDER by c.id_client
                                        """)
            print(curs.fetchall())
            # получаем строки
            update_client(curs, 4, "Сергей", None, 'отец@писателя.com')
            # удаляем номер
            delete_phone(curs, '8566632')
            # удаляем клиента
            delete_client(curs, 2)
            # удаляем клиента
            delete_client(curs, 100)
            # находим клиента
            find_client(curs, "Анна")
            find_client(curs, "Пупкин")
            find_client(curs, "mpup@mail.ru")
            find_client(curs, 5588445)
            curs.close()