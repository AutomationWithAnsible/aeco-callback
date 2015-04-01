import logging


class RethinkDBCallBack():
    def __init__(self, host, port, timeout=2):
        self.db_name = "RethinkDB"
        try:
            self.r = __import__('rethinkdb')
            self.db_import = True
        except ImportError:
            self.db_import = False

        # Connection Setting
        self.db_database = "ansible"
        self.db_tables = ["summary", "failure", "changed"]
        self.timeout = int(timeout)
        self.host = str(host)
        self.port = str(port)
        self.db_server = self.host + ":" + self.port  # not used but good for printing
        self.db_status = self._connect()
        self.db_summary_status = False
        self.db_failures_status = False
        self.db_changed_status = False

    def _create_db(self):
        try:
            self.r.db_create(self.db_database).run()
        except Exception, e:
            print "Failed to create database '%s' error: '%s'" % (self.db_database, e)
            return False
        return True

    def _create_table(self, table):
        try:
            self.r.db(self.db_database).table_create(table).run()
        except Exception, e:
            print "Failed to create table '%s' in '%s' error: '%s'" % (table, self.db_database, e)
            return False
        return True

    def _connect(self):
        if self.db_import:
            try:
                self.r.connect(self.host, self.port, timeout=self.timeout).repl()
                list_of_db = self.r.db_list().run()
                if not any(self.db_database == db for db in list_of_db):
                    if not self._create_db():
                        return False
                list_of_tables = self.r.db(self.db_database).table_list().run()
                for table in self.db_tables:
                    if not any(table == list_table for list_table in list_of_tables):
                        if not self._create_table(table):
                            return False
                return True
            except Exception, e:

                logging.error("Failed to initialized rethinkdb server '%s'. Error: '%s'" % (self.db_server, e))
        return False

    def _insert(self, table, data):
        if self.db_status:
            try:
                result = self.r.db(self.db_database).table(table).insert(data).run()
                if result.get("inserted") >= 1:
                    return True

                return False
            except Exception, e:
                logging.error("Inserting data into ES 'failed' because %s" % e)
        return False

    def insert_data(self, data):
        if self.db_status:
            self.db_summary_status = self._insert("summary", data.summaries)
            # Prepare failures for insertion
            if len(data.failures) > 0:
                self.db_failures_status = self._insert("failure", data.failures)
            else:
                # No Failures
                self.db_failures_status = None
            # Prepare changed for insertion
            if len(data.changed) > 0:
                self.db_changed_status = self._insert("changed", data.changed)
            else:
                # No Changed
                self.db_changed_status = None