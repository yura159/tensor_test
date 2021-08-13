import psycopg2
import json


class Database:
    def __init__(self):
        self._connection = psycopg2.connect(user="postgres",
                                            password="1111",
                                            host="localhost",
                                            port="5432",
                                            database="tensor")
        self._types_rec = {
            1: ["cities", "id, name"],
            2: ["offices", "id, name, id_cities"],
            3: ["workers", "id, name, id_offices"]
        }
        self._cursor = self._connection.cursor()

    def get_workers(self, worker_id):
        request = """select workers.name from workers
            where workers.id_offices in
                (select offices.id from offices
                where offices.id_cities =
                    (select offices.id_cities from offices
                    where offices.id = 
                        (select id_offices from workers
                        where workers.id={})))""".format(worker_id)
        self._execute_data(request)
        return self._cursor.fetchall()

    def add_data(self, json_text):
        json_data = []
        try:
            json_data = json.load(json_text)
        except():
            print("bad json")
        if json_data:
            request = ""
            for data in json_data:
                request += self._get_request(data)
            self._execute_data(request)

    def _execute_data(self, request):
        self._cursor.execute(request)
        self._connection.commit()

    def _get_request(self, data):
        type_rec = data.get("Type")
        name_table = self._types_rec.get(type_rec)[0]
        columns = self._types_rec.get(type_rec)[1]
        return "INSERT INTO {} ({}) VALUES ({});".format(
            name_table, columns,
            Database._get_values(data, type_rec)
        )

    @staticmethod
    def _get_values(data, type_rec):
        rec_id = f"{data.get('id')}, "
        name = f"'{data.get('Name')}'"
        parent_id = ""
        if type_rec != 1:
            parent_id = f", {data.get('ParentId')}"
        return "{}{}{}".format(rec_id, name, parent_id)
