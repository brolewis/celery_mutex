'''Create a Celery ABC that prevents task stacking.'''
## Standard Library
import inspect
import time
## Third Party
import kazoo.client
import celery


class MutexTask(celery.Task):
    '''
    Mutex task to prevent task stacking. The first task wins; any subsequent
    tasks are cancelled.

    Optional parameter:
        - mutex_keys (tuple of strings) - A list of the keys (args or kwargs)
                                          from the task definition that refine
                                          the mutex.
        - mutex_timeout (integer) - Time, in seconds, when the mutex should
                                    expire.
    '''
    abstract = True

    def _get_node(self, args, kwargs):
        '''Get the lock node from the function arguments.'''
        mutex_keys = getattr(self, 'mutex_keys', ())
        lock_node = u'/mutex/celery/{}'.format(self.name)
        items = inspect.getcallargs(self.run, *args, **kwargs)
        for value in (unicode(items[x]) for x in mutex_keys if items.get(x)):
            ## This replace here converts a slash into a fraction-slash.
            ## They look the same but ZooKeeper uses slashes to denote a
            ## new node and since a value could contain a slash (e.g. a
            ## uri) we want to make it into a non-reserved character.
            lock_node += u'.{}'.format(value.replace('/', u'\u2044'))
        return lock_node

    def apply_async(self, args=None, kwargs=None, **options):
        '''Apply the task asynchronously.'''
        conf = self._get_app().conf
        global_timeout = getattr(conf, 'MUTEX_TIMEOUT', None)
        items = inspect.getcallargs(self.run, *args, **kwargs)
        timeout = items.get('mutex_timeout') or global_timeout or 3600
        success = False
        try:
            hosts = conf.ZOOKEEPER_HOSTS
            client = kazoo.client.KazooClient(hosts=hosts)
            client.start()
            lock_node = self._get_node(args, kwargs)
            if client.exists(lock_node):
                if time.time() - float(client.get(lock_node)[0]) > timeout:
                    client.delete(lock_node)
                    success = True
            else:
                success = True
        except Exception as exc:
            print 'Error stopping execution: {}'.format(exc)
            return
        else:
            if success:
                client.create(lock_node, str(time.time()), makepath=True)
                return super(MutexTask, self).apply_async(args, kwargs,
                                                          **options)
            else:
                print 'This task has been locked.'
                return
        finally:
            try:
                client.stop()
                client.close()
            except Exception:
                pass

    def __call__(self, *args, **kwargs):
        '''Direct method call.'''
        local = self.request.called_directly or self.request.is_eager
        if local:
            conf = self._get_app().conf
            global_timeout = getattr(conf, 'MUTEX_TIMEOUT', None)
            items = inspect.getcallargs(self.run, *args, **kwargs)
            timeout = items.get('mutex_timeout') or global_timeout or 3600
            success = False
            try:
                hosts = conf.ZOOKEEPER_HOSTS
                client = kazoo.client.KazooClient(hosts=hosts)
                client.start()
                lock_node = self._get_node(args, kwargs)
                if client.exists(lock_node):
                    if time.time() - float(client.get(lock_node)[0]) > timeout:
                        client.delete(lock_node)
                        success = True
                else:
                    success = True
            except Exception as exc:
                print 'Error stopping execution: {}'.format(exc)
                return
            else:
                if success:
                    client.create(lock_node, str(time.time()), makepath=True)
                    ret = super(MutexTask, self).__call__(*args, **kwargs)
                    client.delete(lock_node)
                    return ret
                else:
                    print 'This task has been locked.'
                    return
            finally:
                try:
                    client.stop()
                    client.close()
                except Exception:
                    pass
        else:
            return super(MutexTask, self).__call__(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        '''Delete lock node of task, regardles of status.'''
        if not self.request.called_directly or self.request.is_eager:
            ## Only remove the lock if the job was not called locally
            client = None
            try:
                hosts = self._get_app().conf.ZOOKEEPER_HOSTS
                client = kazoo.client.KazooClient(hosts=hosts)
                client.start()
                lock_node = self._get_node(args, kwargs)
                if client.exists(lock_node):
                    client.delete(lock_node)
            finally:
                if hasattr(client, 'stop'):
                    client.stop()
                    client.close()
