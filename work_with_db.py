import psycopg2
import json


class Database:
    def __init__(self):
        try:
            config = Database._get_json("config.json")
            data_database = config.get("database")
            self._connection = psycopg2.connect(
                user=data_database.get("user"),
                password=data_database.get("password"),
                host=data_database.get("host"),
                port=data_database.get("port"),
                database=data_database.get("name_database")
            )
            self._types_rec = config.get("types_rec")
            self._cursor = self._connection.cursor()
        except():
            print("Подключение не удалось")

    def get_workers(self, worker_id):
        """
        About: метод реализовывает выборку всех сотрудников по указанному идентификатору сотрудника
        :param worker_id: id сотрудника
        :return: список сотрудников, работающих в одном офисе с worker_id
        """
        # запрос на получение списка нужных сотрудников
        request = """
            select workers.name from workers
            where workers.id_offices in
                (select offices.id from offices
                where offices.id_cities =
                    (select offices.id_cities from offices
                    where offices.id = 
                        (select id_offices from workers
                        where workers.id={})))
        """.format(worker_id)
        self._execute_data(request)
        return [name[0] for name in self._cursor.fetchall()]

    def add_data(self, path):
        """
        About: метод импортирует данные по организациям в таблицы
        :param path: информация о сотрудника в json формате
        """
        try:
            json_data = Database._get_json(path)
        except():
            json_data = []
        # если данные из json_text получены успешно, создаем набор запросов на добавление данных в таблицы
        if json_data:
            request = ""
            for data in json_data:
                request += self._get_insert_request(data)
            self._execute_data(request)

    def _execute_data(self, request):
        """
        About: Метод отправялет запрос в базу данных, а после коммитит изменения в бд
        :param request: запрос, который нужно отправить в базу данных
        """
        self._cursor.execute(request)
        self._connection.commit()

    def _get_insert_request(self, data):
        """
        About: метод формирует запрос на добавление записи в опр. таблицу в бд
        :param data: данные о оргонизации/пользователе
        :return: запрос на добавление данных в опр. таблицу
        """
        type_rec = str(data.get("Type"))
        name_table = self._types_rec.get(type_rec)[0]
        columns = self._types_rec.get(type_rec)[1]
        return "INSERT INTO {} ({}) VALUES ({});".format(name_table, columns, Database._get_values(data, type_rec))

    @staticmethod
    def _get_json(path):
        """
        About: парсин json файла
        :return: json
        """
        with open(path, "r", encoding="UTF-8") as json_file:
            return json.load(json_file)

    @staticmethod
    def _get_values(data, type_rec):
        """
        About: метод перобразовывает добавляемые данные в строковое представление
        :param data: данные о оргонизации/пользователе
        :param type_rec: тип запроса
        :return: возвращает набор добавляемых значение в виде строки
        """
        rec_id = data.get('id')
        name = data.get('Name')
        parent_id = data.get('ParentId')
        if type_rec != 1:
            return "{},{},{}".format(rec_id, name, parent_id)
        return "{},{}".format(rec_id, name)
