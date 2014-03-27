Celey Mutex
===========

Celery Mutex is a mutex for Celery Tasks, optionally refined based on provided
keys. This mutex prevents execution of concurrent tasks, as opposed to delaying
execution.


Installation
------------

Simply run `pip install celery_mutex`


Requirements
------------

Celery Mutex relies on [ZooKeeper](http://zookeeper.apache.org/).


Usage
-----

Celery Mutex takes advantage of Abstract Tasks for Celery. To utilize this
abstract task, add it as a base for the task:

    import celery_mutex

    @app.task(base=celery_mutex.MutexTask)
    def my_task(a, b):
        return a + b

A new configuration variable is required in order to let Celery Mutex know
about your ZooKeeper servers:

    ZOOKEEPER_HOSTS = 'localhost:2181'

This is a comma-separated list of hosts to connect to.

By default a mutex times out after one hour. This can be changed globally by
setting `MUTEX_TIMEOUT` or per-task by setting `mutex_timeout` on the task. For
both the value is an integer for the number of seconds to set the time out.

A second configuration option allows you to refine the mutex for a given task.
By default, Celery Mutex only allows one instance of a task at a time. However,
there may be a need to further refine what is controlled by the mutex. This can
be done by setting `mutex_keys` on the task. The value is a list of keys that
are to be used for determining exclusivity.

Using our above example, adding the two optional parameters would yield:

    import celery_mutex

    @app.task(base=celery_mutex.MutexTask, mutex_timeout=30, mutex_keys=('a',))
    def my_task(a, b):
        return a + b

This would cause the mutex to only prevent execution for tasks that share the
same value for "a".
