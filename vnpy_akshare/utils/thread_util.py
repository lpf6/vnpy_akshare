import threading
from functools import partial
from queue import Empty, Queue
from threading import Semaphore, current_thread

import dask
import joblib
import numpy as np
import pandas as pd
from joblib import Parallel

from .dask_utils import init_client
from .log import log

DEFAULT_BACKEND = 'dask'


def main_run(task, result: list, lock: Semaphore):
    res = task()
    result.append(res)
    lock.release()


def main_task(func, tasks, result: list, lock: Semaphore, count, backend):
    res = func(tasks, backend, count, batch_size=20)
    result.extend(res)
    lock.release()


def parallel_execute(tasks, backend, count=None, batch_size: str or int = "auto"):
    if backend == "dask":
        init_client()
        return dask.compute(*tasks(backend))
        # with joblib.parallel_backend(backend):
        #     return Parallel(n_jobs=count, batch_size=batch_size, verbose=10)(tasks(backend=backend))
    else:
        if backend == 'ray':
            import ray
            from ray.util.joblib import register_ray
            ray.init(address='auto', redis_password='5241590000000000')
            register_ray()
            count = int(ray.state.cluster_resources()["CPU"]) * 1.3
            log.info("num {}", count)
            batch_size = 'auto'
        try:
            with joblib.parallel_backend(backend):
                return Parallel(n_jobs=count, batch_size=batch_size, verbose=10)(tasks(backend=backend))
        finally:
            if backend == 'ray':
                import ray
                ray.shutdown()


def parallelize_dataframe_tasks(func, df_split, backend=None):
    if backend == "dask":
        import dask.delayed as delayed
    else:
        from joblib.parallel import delayed
    return (delayed(func)(d) for d in df_split)


def parallelize_dataframe(df, func, n_cores=None):
    df_split = np.array_split(df, n_cores if n_cores else max(len(df) / 10000, 1))
    df = pd.concat(list(main_thread_parallel(partial(parallelize_dataframe_tasks, func, df_split))))
    return df


def execute_main(func, tasks, count=None, backend=None):
    backend = backend if backend else DEFAULT_BACKEND
    if count is None:
        count = int(joblib.cpu_count() * 1.3)
    else:
        count = int(count)
    batch_size = 20
    if isinstance(current_thread(), threading._MainThread):
        return func(tasks, backend, count, batch_size)
    result = []
    cond = Semaphore(0)
    post_main_thread(partial(main_task, func, tasks, result, cond, count, backend))
    cond.acquire()

    return result


def data_persist(data, backend=None):
    backend = backend if backend else DEFAULT_BACKEND
    if backend == 'dask':
        from dask import dataframe
        init_client()
        return dataframe.from_pandas(data, chunksize=100000)
    return data


def main_thread_parallel(tasks, count=None, backend=None):
    # backend = "loky"
    # backend = 'multiprocessing'
    return execute_main(parallel_execute, tasks, count=count, backend=backend)


# somewhere accessible to both:
callback_queue = Queue()


def _main_task(func_to_call_from_main_thread, result, cond):
    res = func_to_call_from_main_thread()
    result.append(res)
    cond.release()


def run_on_main_thread(func_to_call_from_main_thread, block=False):
    if isinstance(current_thread(), threading._MainThread):
        return func_to_call_from_main_thread()
    result = []
    cond = Semaphore(0)
    post_main_thread(partial(_main_task, func_to_call_from_main_thread, result, cond), block)
    cond.acquire()

    return result[0]


def post_main_thread(func_to_call_from_main_thread, block=False):
    if block:
        cond = Semaphore(0)
        result = []
        callback_queue.put(partial(main_run, func_to_call_from_main_thread, result, cond))
        cond.acquire()
        return result[0]
    else:
        callback_queue.put(func_to_call_from_main_thread)


def loop_once(block=False):
    try:
        callback = callback_queue.get(block)  # blocks until an item is available
        callback()
    except Empty:
        pass


def loop(count=-1):
    n = 0
    while True:
        n += 1
        callback = callback_queue.get(block=True)
        log.info("loop: %s" % n)
        callback()
        if n == count:
            break
