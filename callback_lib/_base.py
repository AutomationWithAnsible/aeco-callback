import os
import sys
import logging
import time
import shutil
import socket
import getpass
import ConfigParser
import simplejson as json
from datetime import datetime
from tabulate import tabulate

temp_dir = "/tmp/"


# Return color based on status
def get_color(status):
    if status is False:
        return "{}{}{}".format('\033[91m', str(status), '\033[0m')
    elif status is True:
        return "{}{}{}".format('\033[33m', str(status), '\033[0m')
    else:
        return "{}{}{}".format('\033[92m', str(status), '\033[0m')


def get_time_rfc_3339():
    time_run = datetime.utcnow()
    return time_run.isoformat("T") + "Z"


def make_dir(dir_name):
    if not os.path.isdir(dir_name):
        try:
            os.makedirs(dir_name)
        except Exception as e:
            logging.error("Unable to create directory '%s' error:'%s'" % (dir_name, e))
            return False
    return True


def log_setup(debug_level, log_file=False, log_stdout=False):
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    kwargs = {}
    if log_file:
        make_dir(os.path.dirname(log_file))
        kwargs.update({"filename": log_file})
    elif log_stdout:
        kwargs.update({"filename": logging.StreamHandler(sys.stdout)})
    log_level = getattr(logging, debug_level)
    kwargs.update({"level": log_level})
    kwargs.update({"format": "%(asctime)s - %(name)s - %(levelname)s - %(lineno)s - %(module)s : %(message)s"})
    logging.basicConfig(**kwargs)


class TaskFile():
    dir_failure = temp_dir + "/aeco_fail"
    file_failure = dir_failure + "/" + "fail_log_%s_callback.json" % os.getpid()
    dir_changed = temp_dir + "/aeco_changed"
    file_changed = dir_changed + "/" + "changed_log_%s_callback.json" % os.getpid()

    @classmethod
    def clean_folders(cls):
        # Clean failure dir
        if os.path.isdir(cls.dir_failure):
            shutil.rmtree(cls.dir_failure + "/")
        # Clean changed dir
        if os.path.isdir(cls.dir_changed):
            shutil.rmtree(cls.dir_changed + "/")

    @staticmethod
    def _write_to_file(file_path, data):
        try:
            with open(file_path, 'at') as the_file:
                the_file.write(json.dumps(data) + "\n")
        except Exception, e:
            logging.error("Write to file failed '%s' error:'%s'" % (file_path, e))

    @classmethod
    def _read_from_file(cls, file_path):
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'rt') as f:
                    lines = f.readlines()
                return lines
            except IOError, e:
                logging.error("Reading file failed '%s' I/O error: %s'" % (file_path, e))
            except Exception as e:
                logging.error("Reading file failed '%s' error: %s'" % (file_path, e))
        return False

    @classmethod
    def write_failures_to_file(cls, host, res, ignore_errors, name=None):
        failure_data = dict()
        invocation = res.get("invocation", {})
        failure_data["fail_module_args"] = invocation.get("module_args", "")
        failure_data["fail_module_name"] = invocation.get("module_name", "")
        failure_data["ignore_error"] = ignore_errors
        if name:
            failure_data["task_name"] = name
        failure_data["host"] = host
        failure_data["fail_msg"] = res.get("msg", "")
        cls._write_to_file(cls.file_failure, failure_data)

    @classmethod
    def write_changed_to_file(cls, host, res, name=None):
        changed_data = dict()
        invocation = res.get("invocation", {})
        changed_data["changed_module_args"] = invocation.get("module_args", "")
        changed_data["changed_module_name"] = invocation.get("module_name", "")
        changed_data["host"] = host
        if name:
            changed_data["task_name"] = name
        changed_data["changed_msg"] = res.get("msg", "")
        cls._write_to_file(cls.file_changed, changed_data)

    @classmethod
    def read_failures_from_file(cls):
        return cls._read_from_file(cls.file_failure)

    @classmethod
    def read_changed_from_file(cls):
        return cls._read_from_file(cls.file_changed)


class AecoCallBack(object):
    class PlayTimer():
        def __init__(self):
            self.timer_start = time.time()
            self.timer_stop = 0
            self.timer_run = 0

        def stop(self):
            self.timer_stop = time.time()
            self.timer_run = format(self.timer_stop - self.timer_start, '.2f')

    class AnsibleStats():
        def __init__(self):
            self.failures = []
            self.changed = []
            self.summaries = []
            self.common = dict()
            self.clock = None  # Timer for run

        def set_common_parm(self, playbook):
            self.common["ansible_version"] = playbook._ansible_version.get("full")
            self.common["check"] = playbook.check
            self.common["play_name"] = playbook.filename
            self.common["only_tags"] = playbook.only_tags
            self.common["skip_tags"] = playbook.skip_tags
            self.common["transport"] = playbook.transport
            self.common["workstation"] = socket.gethostname()
            self.common["user"] = getpass.getuser()
            self.common["extra_vars"] = playbook.extra_vars
            self.common["timeout"] = playbook.timeout

        def set_changed(self):
            timestamp = get_time_rfc_3339()
            lines = TaskFile().read_changed_from_file()

            if not lines:
                return False
            for line in lines:
                json_line = json.loads(line)  # convert json to dic
                changed = dict()
                changed["host"] = json_line.get("host")
                changed["@timestamp"] = timestamp
                changed["changed_msg"] = json_line.get("changed_msg")
                changed["changed_module_name"] = json_line.get("changed_module_name")
                changed["changed_module_args"] = json_line.get("changed_module_args")
                if json_line.get("task_name", False):
                    changed["task_name"] = json_line.get("task_name")
                changed.update(self.common)
                self.changed.append(changed)

        def set_failures(self):
            timestamp = get_time_rfc_3339()
            lines = TaskFile().read_failures_from_file()

            if not lines:
                return False
            for line in lines:
                json_line = json.loads(line)  # convert json to dic
                failure = dict()
                failure["host"] = json_line.get("host")
                failure["@timestamp"] = timestamp
                failure["fail_msg"] = json_line.get("fail_msg")
                failure["fail_module_name"] = json_line.get("fail_module_name")
                failure["ignore_error"] = json_line.get("ignore_error")
                failure["fail_module_args"] = json_line.get("fail_module_args")
                if json_line.get("task_name", False):
                    failure["task_name"] = json_line.get("task_name")

                failure.update(self.common)
                self.failures.append(failure)

        def set_stats(self, stats):
            timestamp = get_time_rfc_3339()
            for host in stats.processed.keys():
                summary = dict()
                summary["host"] = host
                summary["@timestamp"] = timestamp
                summary["time"] = self.clock.timer_run
                summary.update(stats.summarize(host))  # ok,changed,failed, unreachable
                summary.update(self.common)
                self.summaries.append(summary)

    def __init__(self):
        make_dir(TaskFile().dir_failure)
        make_dir(TaskFile().dir_changed)
        self.stats = self.AnsibleStats()
        self.dbs = []
        self.db_settings = []

    def db_insert(self):
        for db_setting in self.db_settings:
            new_db = db_setting["name"](**db_setting["init"])
            new_db.insert_data(self.stats)
            self.dbs.append(new_db)  # register new instances

    def print_summary(self):
        headers = ["", ""]
        display = []
        display.append(["CB--> Execution time", "[ " + self.stats.clock.timer_run + " ]"])
        i = 1
        for db in self.dbs:
            display.append(["{}) {} Connection ".format(i, db.db_name), "[ " + get_color(db.db_status) + " ]"])
            if db.db_status:  # Check db status
                # Status of summary
                display.append(["...../ Summary insert", "[ " + get_color(db.db_summary_status) + " ]"])
                # Status of summary
                display.append(["...../ Changed insert", "[ " + get_color(db.db_changed_status) + " ]"])
                # Status of failures
                display.append(["...../ Failures insert", "[ " + get_color(db.db_failures_status) + " ]"])
            i += 1
        print tabulate(display, headers=headers)


def recurse_parse_section(config, section):
    section_result = {}
    for (each_key, each_value) in config.items(section):
        section_result.update({each_key: each_value})
    return section_result


def parse_config(path_to_me, __CONF__,):
    config = ConfigParser.ConfigParser()
    config_file = path_to_me + __CONF__
    try:
        config.read(config_file)
        result = {}
        for section in config.sections():
            result.update({section: recurse_parse_section(config, section)})
        return result

    except ConfigParser.NoSectionError as E:
        print "Config error %s" % E

