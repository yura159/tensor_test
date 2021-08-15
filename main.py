from work_with_db import Database


def get_worker(database):
    """
    About: метод для получения выборки всех сотрудников по указанному идентификатору сотрудника
    :param database: база данных, в которую мы будем добавлять данные
    """
    print("Введите id сотрудника")
    id_workers = int(input())
    workers = database.get_workers(id_workers)
    print("Сотрудники:")
    [print(worker) for worker in workers]


my_db = Database()
# my_db.add_data("data.json") # импортирование данных в бд
get_worker(my_db)
