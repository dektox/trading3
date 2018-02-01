import psycopg2


class Postgres:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None

        self.connection = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.dbname,
            user=self.config.user,
            password=self.config.password,
        )
        self.cursor = self.connection.cursor()

    def initialize(self):
        return True

    def destroy(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

        return True

    def commit(self):
        self.connection.commit()
        return True

    def rollback(self):
        self.connection.rollback()
        return True

    def execute(self, sql, data=None):
        try:
            self.cursor.execute(sql, data)
            data = self.cursor.fetchall()
            return data
        except Exception as e:
            if 'no results to fetch' in str(e):
                return []
            else:
                raise

    def execute_nofetch(self, sql, data=None):
        self.cursor.execute(sql, data)
