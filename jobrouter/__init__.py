import os
import inspect
import asyncio
import pathlib
import importlib.util
from typing import Optional
from functools import wraps

def job(name: Optional[str] = None, description: Optional[str] = None):
    def decorator(func):
        job_metadata = {
            "name": name,
            "description": description,
        }

        func.job_metadata = job_metadata

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator

class JobRequest:
    def __init__(self, name: str, args: dict = None):
        self.name = name
        self.args = args or {}

class Jobs:
    __instance = None

    def __new__(cls, dir, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(Jobs, cls).__new__(cls)
            cls.__instance.__init_once(dir, *args, **kwargs)
        return cls.__instance

    def __init_once(self, dir):
        if getattr(self, "_is_initialized", False):
            return
        self._is_initialized = True

        self.directory = os.path.abspath(dir)
        self.jobs = []
        self._get_jobs()

    def _get_jobs(self):
        job_files = self._find_job_files()
        for filepath in job_files:
            self._load_jobs_from_file(filepath)

    def _find_job_files(self):
        return [
            filepath
            for filepath in pathlib.Path(self.directory).rglob("*.py")
            if filepath.name != "__init__.py"
        ]

    def _load_jobs_from_file(self, filepath):
        module_name = os.path.splitext(os.path.basename(filepath))[0]
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        module = importlib.util.module_from_spec(spec)

        if spec.loader:
            spec.loader.exec_module(module)
            self._extract_jobs_from_module(module)

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

    # simple base router:
    async def router(self, job_request: JobRequest):
        """
        receives a JobRequest model/dictionary, routes the job from the websocket, forwards the JobRequest arguments to the job to be run. 
        
        jobs can be ONLY async functions and can either return or yield results. meaning we have to handle base returns and returned generators.

        job_request: basic job definition, the job being requested
        """
        job = next((job for job in self.jobs if job.get("name") == job_request.name), None)
        if not job:
            raise FileNotFoundError(f"The job '{job_request.name}' was not found in {self.directory}.")

        job_func = job.get("function")

        if inspect.iscoroutinefunction(job_func):
            return await job_func(**job_request.args)
        else:
            raise # Error that says functions must be async for jobs.

    # Routers for different use cases:
    def websocket_router():
        # receives a websocket, routes the job from the websocket, forwards the websocket onto the job to be run.
        # TODO
        pass

    def binary_stream_router():
        # receives a binary stream, routes the job from the binary stream, passes the binary stream onto the job to be run.
        # TODO
        pass

'''
Example and ideal use case:
from pyjobs import Jobs, job

jobs = Jobs(dir='./')

@job(
    name="jobs_list",
    description="List out all available jobs the server can run.",
)
async def main():
    return [{**j, 'function': j['function'].__name__} for j in jobs.jobs]

job = JobRequest(name='job_list')
result = jobs.router(job)
print(result) # [{'name': 'jobs_list', 'description': 'List out all available jobs the server can run.'}, {'name': 'addition', 'description': 'Add two numbers together'}]

@job(
    name="addition",
    description="Add two numbers together",
)
async def main(num1: int, num2: int):
    return num1 + num2

job = JobRequest(name='addition', args={'num1': 4, 'num2': 9})
result = jobs.router(job)
print(result) # 13
'''
