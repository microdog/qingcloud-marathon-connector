#!/usr/bin/env python
"""Connector of Mesosphere Marathon and QingCloud load balancer."""

import os
import json
import logging
import threading
import collections

__license__ = '''
The MIT License (MIT)

Copyright (c) 2015 Microdog <kainan.zhu@outlook.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
'''

# Web application definition
AppInfo = collections.namedtuple('AppInfo', [
    'app_id', 'load_balancer_id', 'listener_id', 'policy_id', 'weight', 'vxnet_id'
])

# QingCloud backend
BackendInfo = collections.namedtuple('BackendInfo', [
    'name', 'resource_id', 'port', 'weight', 'policy_id', 'backend_id'
])

# Marathon task
TaskInfo = collections.namedtuple('TaskInfo', [
    'task_id', 'host', 'port', 'app_id', 'event'
])
TaskInfo.EVENT_START = 0
TaskInfo.EVENT_STOP = 1


class QingCloudClient(object):
    """QingCloud IAAS client."""

    @staticmethod
    def _load_backend(data):
        """Load BackendInfo from QingCloud backend data.

        :param data: QingCloud backend data
        :return: BackendInfo object
        """
        return BackendInfo(
            backend_id=data.get('loadbalancer_backend_id'),
            weight=data['weight'],
            resource_id=data['resource_id'],
            name=data.get('loadbalancer_backend_name'),
            port=data['port'],
            policy_id=data.get('loadbalancer_policy_id')
        )

    @staticmethod
    def _dump_backend(backend):
        """Dump BackendInfo to QingCloud backend data.

        :param backend: object of BackendInfo
        :return: dict
        """
        data = {
            'resource_id': backend.resource_id,
            'port': backend.port,
            'weight': backend.weight
        }

        if backend.name:
            data['loadbalancer_backend_name'] = backend.name
        if backend.policy_id:
            data['loadbalancer_policy_id'] = backend.policy_id

        return data

    def __init__(self, config):
        self._debug = config.get('DEBUG', False)
        self._zone_id = config['QY_ZONE_ID']
        self._access_key = config['QY_ACCESS_KEY_ID']
        self._secret_key = config['QY_SECRET_ACCESS_KEY']

        self._logger = logging.getLogger('QingCloudClient')

        self._connect()

    def _connect(self):
        import qingcloud.iaas
        self._conn = qingcloud.iaas.connect_to_zone(
            self._zone_id, self._access_key, self._secret_key
        )
        self._conn.debug = self._debug
        self._logger.info('QingCloud initialized.')

    def _check_result(self, result, accepted=None):
        """Check QingCloud result.

        :param result: QingCloud result
        :param accepted: list of accepted return codes
        :return: bool. True if result accepted.
        """
        if accepted is None:
            accepted = [0]
        if 0 not in accepted:
            accepted.append(0)

        self._logger.debug('QingCloud result: %s', result)

        if result is None:
            self._logger.warning('QingCloud request failed due to network error')
            return False

        ret_code = result.get('ret_code', -1)
        if ret_code not in accepted:
            self._logger.warning('QingCloud request failed with return code: %s, %s', ret_code, result.get('message'))
            return False

        return True

    def get_resource_id_by_dns_alias(self, alias):
        """Get resource-id by DNS alias.

        :param alias: dns alias
        :return: resource-id or None if not found
        """
        result = self._conn.describe_dns_aliases(search_word=alias)
        if self._check_result(result):
            for dns_alias in result['dns_alias_set']:
                if dns_alias['dns_alias_name'] == alias:
                    return dns_alias['resource_id']
            return None
        else:
            return None

    def get_instance_infos_by_vxnet(self, vxnet_id):
        """Get instances' id and private ip of a vxnet.

        :param vxnet_id: vxnet id
        :return: dict of private ip and instance id
        """
        instances = {}

        offset = 0
        eol = False
        while not eol:
            result = self._conn.describe_vxnet_instances(vxnet_id, offset=offset)
            if self._check_result(result):
                for instance in result['instance_set']:
                    instances[instance['private_ip']] = instance['instance_id']
                eol = result['total_count'] == len(instances)
                offset += len(result['instance_set'])
            else:
                return None

        return instances

    def get_backends_by_listener(self, listener_id):
        """Get backends of a listener.

        :param listener_id: listener id
        :return: list of BackendInfo or None if request failed.
        """
        result = self._conn.describe_loadbalancer_backends(
            loadbalancer_listener=listener_id
        )
        if self._check_result(result):
            return map(self._load_backend, result['loadbalancer_backend_set'])
        else:
            return None

    def add_backends_to_listener(self, listener_id, backends):
        """Add backends to listener.

        :param listener_id: listener id
        :param backends: list of BackendInfo
        :return: list of backend ids or None if request failed.
        """
        if not isinstance(backends, list):
            backends = [backends]
        backends = map(self._dump_backend, backends)
        result = self._conn.add_backends_to_listener(
            loadbalancer_listener=listener_id,
            backends=backends
        )
        if self._check_result(result):
            return result['loadbalancer_backends']
        else:
            return None

    def delete_backends(self, backend_ids):
        """Delete backends.

        :param backend_ids: list of backend ids
        :return: deleted backend ids or None if request failed.
        """
        if not isinstance(backend_ids, list):
            backend_ids = [backend_ids]
        result = self._conn.delete_loadbalancer_backends(
            loadbalancer_backends=backend_ids
        )
        if self._check_result(result):
            return result['loadbalancer_backends']
        else:
            return None

    def update_load_balancer(self, load_balancer_ids):
        """Apply changes of load balancers.

        :param load_balancer_ids: load balancer ids
        :return: job id or None if request failed.
        """
        if not isinstance(load_balancer_ids, list):
            load_balancer_ids = [load_balancer_ids]
        result = self._conn.update_loadbalancers(
            loadbalancers=load_balancer_ids
        )
        if self._check_result(result):
            return result['job_id']
        else:
            return None


class EventHandler(object):
    """Event handler."""

    def __init__(self, apps, config):
        # Prepare app info
        self._apps = apps
        self._build_indexes()

        # Setup thread
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True

        # Processing control
        self._running = False
        self._stop_mercy = config.get('HANDLER_STOP_MERCY', 10)
        self._starting = threading.Event()
        self._starting_threshold = config.get('HANDLER_STARTING_THRESHOLD', 3)
        self._starting_timeout = config.get('HANDLER_STARTING_TIMEOUT', 5)

        # Task queues
        self._task_queue = collections.deque()
        self._task_queue_lock = threading.Lock()
        self._delayed_queue = []
        self._delayed_app_ids = set()

        # Logging
        self._logger = logging.getLogger('EventHandler')

        # Task statuses
        self._task_starting_status = config.get('TASK_STARTING_STATUS', 'TASK_RUNNING')
        self._task_stopping_statuses = config.get('TASK_STOPPING_STATUSES',
                                                  ('TASK_FINISHED', 'TASK_FAILED', 'TASK_KILLED', 'TASK_LOST'))

        # QingCloud client
        self._qingcloud = QingCloudClient(config)

        # Resource id cache
        self._resource_id_cache = collections.defaultdict(dict)

    def _build_indexes(self):
        self._apps_index = {
            app.app_id: app for app in self._apps
            }

    def _run(self):
        """Main loop."""
        self._logger.info('Event handler running...')

        while self._running:
            self._starting.wait(self._starting_timeout)
            if not self._running:
                break

            delayed_tasks = self._delayed_queue
            self._delayed_queue = []
            self._delayed_app_ids = set()

            with self._task_queue_lock:
                for task in delayed_tasks:
                    self._task_queue.appendleft(task)
                task_info = self._task_queue.popleft() if self._task_queue else None

            while task_info:
                if task_info.app_id in self._delayed_app_ids:
                    self._logger.debug('Task delayed for app %s', task_info.app_id)
                    self._delay_tasks([task_info])
                else:
                    # Get a batch of task infos having the same event
                    task_event = task_info.event
                    tasks = [task_info]
                    while len(self._task_queue) > 0 and self._task_queue[0].event == task_event:
                        tasks.append(self._task_queue.popleft())

                    # Handle these task infos
                    try:
                        self._process_tasks(tasks, task_event)
                    except Exception as e:
                        self._logger.error('Exception got while processing task infos', exc_info=True)

                with self._task_queue_lock:
                    task_info = self._task_queue.popleft() if self._task_queue else None

            self._starting.clear()

    def _process_tasks(self, task_infos, event):
        """Dispatch tasks to starting handler or stopping handler."""
        if event == TaskInfo.EVENT_START:
            self._process_start_event(task_infos)
        elif event == TaskInfo.EVENT_STOP:
            self._process_stop_event(task_infos)

    def _process_start_event(self, task_infos):
        # Get hosts' resource ids and map to apps
        app_tasks = self._map_tasks(task_infos)

        for app, tasks in app_tasks.iteritems():
            self._logger.debug('Processing %s starting task(s) for app %s', len(tasks), app.app_id)

            # Get current backends
            backends = self._qingcloud.get_backends_by_listener(app.listener_id)

            # Filter existing tasks
            backends_traits = set(((b.resource_id, b.port) for b in backends))
            tasks = filter(
                lambda t: (self._get_resource_id(app.vxnet_id, t.host), t.port) not in backends_traits,
                tasks
            )
            self._logger.debug('Filtered tasks: %s', tasks)

            backends_new = []
            for task_info in tasks:
                backend = BackendInfo(
                    name='$'.join((task_info.app_id, task_info.task_id)),
                    resource_id=self._get_resource_id(app.vxnet_id, task_info.host),
                    port=task_info.port,
                    weight=app.weight,
                    policy_id=app.policy_id,
                    backend_id=None
                )
                backends_new.append(backend)

            if len(tasks):  # Update load balancer when necessary
                backend_ids_added = self._qingcloud.add_backends_to_listener(app.listener_id, backends_new)
                self._logger.info('Backends added to app %s: %s', app.app_id, backend_ids_added)

                job_id = self._qingcloud.update_load_balancer(app.load_balancer_id)
                if job_id:
                    self._logger.info('Update of load balancer of app %s scheduled with job id %s', app.app_id, job_id)
                else:  # Delay tasks if update action failed
                    self._logger.warning('Cannot perform load balancer update, delay all tasks')
                    self._delay_tasks(tasks)
            else:
                self._logger.debug('No changes need to be performed')

    def _process_stop_event(self, task_infos):
        # Get hosts' resource ids and map to apps
        app_tasks = self._map_tasks(task_infos)

        for app, tasks in app_tasks.iteritems():
            self._logger.debug('Processing %s stopping task(s) for app %s', len(tasks), app.app_id)

            # Get current backends
            backends = self._qingcloud.get_backends_by_listener(app.listener_id)

            # Filter non-existing tasks
            backends_traits = {
                (b.resource_id, b.port): b for b in backends
                }

            backend_ids_delete = []
            for task in tasks:
                trait = (self._get_resource_id(app.vxnet_id, task.host), task.port)
                if trait in backends_traits:
                    backend_ids_delete.append(backends_traits[trait].backend_id)

            if len(backend_ids_delete):  # Update load balancer when necessary
                backend_ids_deleted = self._qingcloud.delete_backends(backend_ids_delete)
                self._logger.info('Backends deleted from app %s: %s', app.app_id, backend_ids_deleted)

                job_id = self._qingcloud.update_load_balancer(app.load_balancer_id)
                if job_id:
                    self._logger.info('Update of load balancer of app %s scheduled with job id %s', app.app_id, job_id)
                else:  # Delay tasks if update action failed
                    self._logger.warning('Cannot perform load balancer update, delay all tasks')
                    self._delay_tasks(tasks)
            else:
                self._logger.debug('No changes need to be performed')

    def _map_tasks(self, task_infos):
        """Map TaskInfo objects to app definitions and cache hosts' resource ids.

        :param task_infos: list of TaskInfo
        :return: dict of AppInfo object and list of TaskInfo objects
        """
        app_tasks = collections.defaultdict(list)
        for task_info in task_infos:
            # Get app definition
            app = self._apps_index.get(task_info.app_id)
            if not app:
                self._logger.warning('No app definition found for app id: %s', task_info.app_id)
                continue

            # Check and cache host's resource id
            if not self._cache_resource_id(app.vxnet_id, task_info.host):
                continue

            app_tasks[app].append(task_info)
        return app_tasks

    def _cache_resource_id(self, vxnet_id, host):
        """Cache host's resource id from iaas.

        :param host: hostname
        :return: host's resource id or None if resource id not found
        """
        if host in self._resource_id_cache[vxnet_id]:
            return self._resource_id_cache[vxnet_id][host]

        instances = self._qingcloud.get_instance_infos_by_vxnet(vxnet_id)
        if not instances:
            self._logger.error('Cannot get instance infos of vxnet %s', vxnet_id)
            return None

        self._resource_id_cache[vxnet_id].update(instances)

        if host not in self._resource_id_cache[vxnet_id]:
            self._logger.warning(
                'Instance resource id cannot be found for host: %s, please check app definition', host)
            return None
        return self._resource_id_cache[vxnet_id][host]

    def _get_resource_id(self, vxnet_id, host):
        """Get host's resource id from cache.

        :param vxnet_id: id of vxnet which the host belongs to
        :param host: hostname
        :return: host's resource id
        """
        return self._resource_id_cache[vxnet_id][host]

    def _delay_tasks(self, task_infos):
        """Delay tasks to next processing loop and mark task's app as delayed.

        :param task_infos: list of TaskInfo objects
        """
        if not isinstance(task_infos, list):
            task_infos = [task_infos]
        self._delayed_queue.extend(task_infos)
        self._delayed_app_ids = self._delayed_app_ids.union((task.app_id for task in task_infos))

    def _add_task(self, task_info):
        """Add task info to task queue and notify handler thread if necessary.

        :param task_info: TaskInfo object
        """
        with self._task_queue_lock:
            self._task_queue.append(task_info)
            if len(self._task_queue) >= self._starting_threshold:
                self._starting.set()

    def start(self):
        self._logger.info('Starting event handler...')

        self._running = True
        self._thread.start()

    def stop(self):
        self._logger.info('Stopping event handler...')
        self._running = False
        self._starting.set()

        self._logger.info('Waiting for event handler...')
        self._thread.join(timeout=self._stop_mercy)
        if self._thread.is_alive():
            self._logger.info('Force quit.')

        self._logger.info('Event handler stopped.')

    def on_event(self, event):
        try:
            event = json.loads(event)
        except ValueError:
            self._logger.warning('Invalid event message got')
            return

        if not event.get('eventType') == 'status_update_event':
            return

        # Parse task status
        task_status = event['taskStatus']
        if task_status == self._task_starting_status:
            task_event = TaskInfo.EVENT_START
        elif task_status in self._task_stopping_statuses:
            task_event = TaskInfo.EVENT_STOP
        else:
            self._logger.warning('Unknown task status got: %s', task_status)
            return

        # Check ports
        if not len(event['ports']) == 1:
            self._logger.error('Only support task with 1 port open currently, %s got', len(event['ports']))
            return

        task_info = TaskInfo(
            task_id=event['taskId'],
            app_id=event['appId'],
            host=event['host'],
            port=event['ports'][0],
            event=task_event
        )

        self._add_task(task_info)


def load_apps():
    """Load AppInfo objects from apps dir.
    Apps dir is `apps` by default and can be overwritten by `APPS_DIR` env.

    :return: list of AppInfo
    """
    import glob

    apps = []

    if 'APPS_DIR' in os.environ:
        apps_dir = os.environ['APPS_DIR']
    else:
        apps_dir = os.path.join(os.path.dirname(__file__), './apps')

    for app_file in glob.glob(os.path.join(apps_dir, './*.json')):
        try:
            apps.append(AppInfo(**json.load(open(app_file))))
        except Exception:
            logging.error('Cannot load app info: %s', app_file, exc_info=True)
            raise

    logging.debug('Loaded app definitions: %s', apps)

    return apps


def main():
    from flask import Flask, request

    # Create flask app and load config
    app = Flask(__name__)
    app.config.from_pyfile('config.py')
    if 'CONF' in os.environ:
        app.config.from_envvar('CONF')

    # Basic logging
    logging.basicConfig(
        level=app.config.get('LOGGING_LEVEL', 'INFO'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Event handler
    event_handler = EventHandler(load_apps(), app.config)
    event_handler.start()

    # HTTP endpoint
    @app.route('/', methods=['POST'])
    def event():
        event_handler.on_event(request.stream.read())
        return ''

    # Start web server
    app.run(
        host=app.config.get('HOST'), port=app.config.get('PORT'),
        use_reloader=False, use_debugger=False
    )

    # Stop event handler
    event_handler.stop()


if __name__ == '__main__':
    main()
