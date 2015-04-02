import sys
import os
from datetime import datetime

path_to_me = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(path_to_me + "/callback_lib"))
from _base import AecoCallBack, TaskFile, log_setup, parse_config

__CONF__ = "/_aeco_callback.ini"
conf_dic = parse_config(path_to_me, __CONF__)
#TODO Not working
log_config = conf_dic.pop("log", {})
log_setup(log_config.get("level", "INFO"), log_file=log_config.get("filename", False),
          log_stdout=log_config.get("stdout", False))

db_settings = []
for key, value in conf_dic.iteritems():
    try:
        baseImport = __import__(key)
        lib2import = getattr(baseImport, value.get("name"))
    except Exception as E:
        print "Error cant import '%s' raw error '%s'" % (lib2import, E)
    else:
        value.pop("name")
        db_settings.append({"name": lib2import, "init": value})


class CallbackModule(object):
    def __init__(self):
        self.current_task = None
        if len(db_settings) > 1:
            self.my_aeco = AecoCallBack()
            self.my_aeco.db_settings = db_settings
            self.enabled = True
        else:
            self.enabled = False

    def on_any(self, *args, **kwargs):
        pass

    def runner_on_failed(self, host, res, ignore_errors=False):
        if self.enabled:
            TaskFile().write_failures_to_file(host, res, ignore_errors, self.current_task)

    def runner_on_ok(self, host, res):
        if self.enabled and res.get("changed"):
            TaskFile().write_changed_to_file(host, res, self.current_task)

    def runner_on_skipped(self, host, item=None):
        pass

    def runner_on_unreachable(self, host, res):
        pass

    def runner_on_no_hosts(self):
        pass

    def runner_on_async_poll(self, host, res, jid, clock):
        pass

    def runner_on_async_ok(self, host, res, jid):
        pass

    def runner_on_async_failed(self, host, res, jid):
        pass

    def playbook_on_start(self):
        if self.enabled:
            self.my_aeco.stats.clock = self.my_aeco.PlayTimer()

    def playbook_on_notify(self, host, handler):
        pass

    def playbook_on_no_hosts_matched(self):
        pass

    def playbook_on_no_hosts_remaining(self):
        pass

    def playbook_on_task_start(self, name, is_conditional):
        self.current_task = name

    def playbook_on_vars_prompt(self, varname, private=True, prompt=None, encrypt=None, confirm=False, salt_size=None, salt=None, default=None):
        pass

    def playbook_on_setup(self):
        pass

    def playbook_on_import_for_host(self, host, imported_file):
        pass

    def playbook_on_not_import_for_host(self, host, missing_file):
        pass

    def playbook_on_play_start(self, name):
        pass

    def playbook_on_stats(self, stats):
        if self.enabled:
            # Stop timer
            self.my_aeco.stats.clock.stop()
            # Set global parm
            self.my_aeco.stats.set_common_parm(self.playbook)
            # Update stats dict
            self.my_aeco.stats.set_stats(stats)
            self.my_aeco.stats.set_changed()
            self.my_aeco.stats.set_failures()
            # db
            self.my_aeco.db_insert()
            # Print summary
            self.my_aeco.print_summary()
            # Do clean up of dir
            TaskFile().clean_folders()
