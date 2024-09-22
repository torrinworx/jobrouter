import os
import inspect
import asyncio
import pathlib
import importlib.util
from typing import Optional
from functools import wraps
from typing import Optional, Any, Union, AsyncGenerator, Callable


def job(name: Optional[str] = None, description: Optional[str] = None):
    def decorator(func: Callable[..., Any]):
        if not asyncio.iscoroutinefunction(func) and not inspect.isasyncgenfunction(
            func
        ):
            raise TypeError(
                "The job decorator can only be applied to asynchronous functions or async generators."
            )

        job_metadata = {
            "name": name,
            "description": description,
        }

        func.job_metadata = job_metadata

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            return async_wrapper
        elif inspect.isasyncgenfunction(func):

            @wraps(func)
            async def async_gen_wrapper(*args, **kwargs):
                async for item in func(*args, **kwargs):
                    yield item

            return async_gen_wrapper

    return decorator


class JobRequest:
    def __init__(
        self,
        name: str,
        args: dict = None,
        binary_stream: Any = None,
        websocket: Any = None,
    ):
        self.name = name
        self.args = args or {}
        self.binary_stream = binary_stream
        self.websocket = websocket


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Jobs(metaclass=SingletonMeta):
    def __init__(self, dir):
        if not hasattr(self, "initialized"):
            self.directory = os.path.abspath(dir)
            self.jobs = []
            self._get_jobs()
            self.initialized = True

    def _get_jobs(self):
        job_files = self._find_job_files()
        for filepath in job_files:
            self._load_jobs_from_file(filepath)

    def _find_job_files(self):
        return [filepath for filepath in pathlib.Path(self.directory).rglob("*.py")]

    def _load_jobs_from_file(self, filepath):
        module_name = os.path.splitext(os.path.basename(filepath))[0]
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        module = importlib.util.module_from_spec(spec)
        try:
            if spec.loader:
                spec.loader.exec_module(module)
            self._extract_jobs_from_module(module)
        except Exception as e:
            print(f"Failed to load module {module_name} from {filepath}: {e}")

    def _extract_jobs_from_module(self, module):
        for attr in dir(module):
            fn = getattr(module, attr)
            if callable(fn) and hasattr(fn, "job_metadata"):
                job_info = getattr(fn, "job_metadata")
                self.jobs.append(
                    {
                        "name": job_info.get("name", fn.__name__),
                        "description": job_info.get("description", ""),
                        "function": fn,
                    }
                )

    async def router(self, job_request: JobRequest) -> Union[Any, AsyncGenerator]:
        """
        The base router that routes the given job request.

        Receives a JobRequest object, finds and routes the specified job using the
        job name, and forwards the JobRequest arguments to the job to be executed.

        Only async functions can be registered as jobs. These jobs can either
        return or yield results.

        Args:
            job_request (JobRequest): The job request with the name and arguments
            of the job to be executed.

        Returns:
            The result of the job function execution or an AsyncGenerator if the
            function yields results.

        Raises:
            FileNotFoundError: If the specified job is not found.
            TypeError: If the specified job function is not asynchronous.
        """
        job = next(
            (job for job in self.jobs if job.get("name") == job_request.name), None
        )

        if not job:
            raise FileNotFoundError(
                f"The job '{job_request.name}' was not found in {self.directory}."
            )

        job_func = job.get("function")

        if not inspect.iscoroutinefunction(job_func) and not inspect.isasyncgenfunction(
            job_func
        ):
            raise TypeError(
                f"The job '{job_request.name}' must be an async function or async generator to be executed."
            )

        job_params = inspect.signature(job_func).parameters
        relevant_args = {k: v for k, v in job_request.args.items() if k in job_params}

        possible_context_args = {
            "binary_stream": "binary_stream",
            "websocket": "websocket",
        }

        for param, attr in possible_context_args.items():
            if param in job_params and hasattr(job_request, attr):
                relevant_args[param] = getattr(job_request, attr)

        result = job_func(**relevant_args)
        if inspect.isasyncgenfunction(job_func):
            return result
        else:
            return await result

    # Routers for different use cases:
    def websocket_router(self):
        # receives a websocket, routes the job from the websocket, forwards the websocket onto the job to be run.
        # TODO: translate websocket to base router: idea, pass in websocket so that jobs can intake websocket.
        pass

    def binary_stream_router(self):
        # receives a binary stream, routes the job from the binary stream, passes the binary stream onto the job to be run.
        # TODO: translate binary stream to base router: idea, pass in binary stream so that jobs can intake binary stream.
        pass
