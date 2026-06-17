from dask.distributed import Client
import dask
from src.config.settings import DASK_SCHEDULER, DASK_N_WORKERS, DASK_THREADS_PER_WORKER

_client = None  # global client instance


def start():
    """
    Start and return a Dask client using project settings.

    Returns
    -------
    dask.distributed.Client
        The active Dask client instance.
    """
    global _client
    if _client is None:
        dask.config.set(scheduler=DASK_SCHEDULER)
        _client = Client(
            n_workers=DASK_N_WORKERS,
            threads_per_worker=DASK_THREADS_PER_WORKER
        )
    return _client


def stop():
    """
    Close the active Dask client if it exists.
    """
    global _client
    if _client is not None:
        _client.close()
        _client = None
