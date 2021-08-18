import psycopg2
import psycopg2.extras
import time


# Параметры для подключения к базе данных
DB_NAME = "workers"
USER = "postgres"
HOST = "localhost"
PASSWORD = "123461"

# Подключение к локальной базе данных
psycopg2.connect(dbname='workers', user='postgres', password='123461', host='localhost')
conn = psycopg2.connect(dbname=DB_NAME, user=USER, host=HOST, password=PASSWORD)


def start():
    # Программа будет работать и ждать ввода данных, пока её не закроют 
    while(True):
        workerId = input("Введите ID сотрудника: ")
        # Проверка вводных данных
        if (not workerId.isdigit()):
            print("ID должно быть целым числом!")
            continue
        # Засекание времени поиска сотрудников
        #start_time = time.perf_counter()

        # Работа с базой данных
        # (cursor_factory=psycopg2.extras.DictCursor) - данное дополнение преобразует данные с БД из тюпла в удобный словарь
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # SQL query, при выполнении которого найдётся корень(офис) по id сотрудника
            # Запрос выполняется рекурсивно до тех пор, пока не найдётся вершина, у которой не будет родителя
            # В конечном итоге получится таблица пути от сотрудника к офису, и мы выбираем только офис(WHERE parentid = null)

            # Поиск офиса только через ID работника
            # Убрать "AND type = 3", чтобы можно было искать не только по ID сотрудника, но и по ID офиса, отдела
            cur.execute(
                f'''WITH RECURSIVE tab1 as
                (SELECT * FROM company WHERE id = {workerId} AND type = 3 
                UNION ALL
                SELECT company.id, company.parentid, company.name, company.type FROM company 
                    JOIN tab1
                        ON tab1.parentid = company.id)
                SELECT * FROM tab1 WHERE parentid is null;''')
            root = cur.fetchone()
            # Запрос вернёт либо корневую вершину, либо None

            if root == None:
                print('Нет сотрудника с таким ID')
                continue

            # Рекурсинвый поиск всех сотрудников офиса
            # Поиск пройдет по всем вершинам и найдёт те, родителем которого является наш офис
            # Затем пройдет также для этих вершин
            # В конечном итоге получится таблица всего офиса со всеми отделами и сотрудниками, и мы выбираем только сотрудников(WHERE type = 3)
            cur.execute(
                f'''WITH RECURSIVE tab1 as
                (SELECT * FROM company 
                WHERE id = {root['id']}
                UNION ALL
                SELECT company.id, company.parentid, company.name, company.type FROM company
                    JOIN tab1
                        ON company.parentid = tab1.id)
                SELECT * FROM tab1 WHERE type = 3;''')
            workers = cur.fetchall()

            # Преобразуем полученный список вершин-сотрудников в строку
            workerStr = ''

            for worker in workers:
                workerStr += worker['name'] + ', '

            print(workerStr[:-2])
            
            #print(time.perf_counter() - start_time, "seconds")


if __name__ == '__main__':
    # Блок, чтобы не вылетала ошибка при прекращении работы программы
    try:
        start()
    except KeyboardInterrupt:
        conn.close()
        print()
        print('Выключаемся...')