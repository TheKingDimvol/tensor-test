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
        print("Введите ID сотрудника:")
        workerId = input()
        # Проверка вводных данных
        if (not workerId.isdigit()):
            print("ID должно быть целым числом!")
            continue
        start_time = time.perf_counter()

        # Работа с базой данных
        # (cursor_factory=psycopg2.extras.DictCursor) - данное дополнение преобразует данные с БД из тюпла в удобный словарь
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # SQL query, при выполнении которого найдётся корень(офис) по id сотрудника
            # Запрос выполняется рекурсивно до тех пор, пока не найдётся вершина, у которой не будет родителя

            # Раскомментить этот запрос, чтобы можно было искать не только по ID сотрудника, но и по ID офиса, отдела
            """
            cur.execute(
                f'''WITH RECURSIVE tab1(id, parentid, name, type) as
                (SELECT * FROM company WHERE id = {workerId}
                UNION ALL
                SELECT t1.* FROM company t1, tab1 
                WHERE tab1.parentid = t1.ID)
                SELECT * FROM tab1 WHERE parentid is null;''')   
            """
            # Поиск офиса только через ID работника
            cur.execute(
                f'''WITH RECURSIVE tab1 as
                (SELECT * FROM company WHERE id = {workerId} AND type = 3 
                UNION ALL
                SELECT t1.* FROM company t1, tab1 
                WHERE tab1.parentid = t1.ID)
                SELECT * FROM tab1 WHERE parentid is null;''')
            root = cur.fetchone()
            # Запрос вернёт либо корневую вершину, либо None

            if root == None:
                print('Нет сотрудника с таким ID')
                continue
            
            # Функция по поиску всех работников этого офиса
            workers = findWorkers(root)

            # Преобразуем полученный список вершин-сотрудников в строку
            workerStr = ''

            for worker in workers:
                workerStr += worker['name'] + ', '

            print(workerStr[:-2])
            print(time.perf_counter() - start_time, "seconds")

# Функция ищет всех сотрудников по офису
def findWorkers(root):
    # Заносим первую вершину в список
    values = [root]

    # Будем проходиться по всему списку, добавлять детей вершин из списка и удалять уже пройденные вершины, которые не являются сотрудниками
    # Цикл окончится, когда в списке останутся только сотрудники
    i = 0
    while i < len(values):
        # Если текущая вершина сотрудник, то оставляем её и идём дальше
        if values[i]['type'] == 3:
            i += 1
            continue
        # Если вершина это отдел или офис, то ищем их детей
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f'''SELECT * FROM company WHERE parentid = {values[i]['id']}''')      
            children = cur.fetchall()
            
            # Добавляем найденных детей в список, по которому проходимся
            for child in children:
                values.append(child)
            # После, удаляем вершину, в которой уже побывали
            del values[i]
    # Возращаем список со всеми сотрудниками
    return values


if __name__ == '__main__':
    start()