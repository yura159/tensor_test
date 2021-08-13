from work_with_db import Database


def add_data(database, path):
    with open(path, 'r', encoding='UTF-8') as data:
        database.add_data(data)


def get_worker(database):
    id_workers = int(input())
    print(database.get_workers(id_workers))


my_db = Database()
