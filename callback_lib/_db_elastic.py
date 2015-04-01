import logging
import time

# http://stackoverflow.com/questions/25447869/logging-using-elasticsearch-py


class ElasticCallBack():
    def __init__(self, host, port, timeout=2):
        self.db_name = "Elasticsearch DB"
        try:
            self.elasticsearch = __import__('elasticsearch')
            self.helpers = __import__('elasticsearch.helpers')
            self.db_import = True
        except ImportError:
            self.db_import = False
        self.index_name = "ansible_log-" + time.strftime('%Y.%m.%d')  # ES index name one per day
        # Connection Setting
        self.timeout = int(timeout)
        self.host = host
        self.port = port
        self.db_server = self.host + ":" + str(self.port)
        self.db_status = self._connect()
        self.db_summary_status = False
        self.db_failures_status = False
        self.db_changed_status = False

    def _connect(self):
        if self.db_import:
            try:
                self.es = self.elasticsearch.Elasticsearch(self.db_server, timeout=self.timeout)
            except Exception, e:

                logging.error("Failed to initialized elasticSearch server '%s'. Exception = %s " % (self.db_server, e))
                return False
            try:
                return self.es.ping()
            except Exception, e:
                logging.error("Failed to get ping from elasticSearch server '%s'. Exception = %s " % (self.db_server, e))
                return False

    def _insert(self, data, doc_type=None):
        if self.db_status:
            try:
                result = self.helpers.helpers.bulk(self.es, data, index=self.index_name, doc_type=doc_type)
                logging.debug("Inserting  into ES result='%s' doc_type='%s' index='%s data='%s'" %
                              (result, doc_type, self.index_name, data))
                if result:
                    return True
            except Exception, e:
                logging.error("Inserting data into ES 'failed' because %s" % e)
        return False

    def insert_data(self, data):
        if self.db_status:
            # 1- Prepare summaries for insertion
            new_summaries = []
            for summary in data.summaries:
                new_summaries.append({"_source": summary})
            self.db_summary_status = self._insert(new_summaries, doc_type="ansible-summary")

            # Prepare failures for insertion
            if len(data.failures) > 0:
                new_failures = []
                for failure in data.failures:
                    new_failures.append({"_source": failure})
                self.db_failures_status = self._insert(new_failures, doc_type="ansible-failure")
            else:
                # No Failures
                self.db_failures_status = None

            # Prepare changed for insertion
            if len(data.changed) > 0:
                new_changed = []
                for change in data.changed:
                    new_changed.append({"_source": change})
                self.db_changed_status = self._insert(new_changed, doc_type="ansible-changed")
            else:
                # No changed
                self.db_changed_status = None