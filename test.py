"""
NOTE: For some stupid reason in order to run this file you need to comment out the stupid setup.py file
because some python process takes over and prevents you from running scripts:

torrin@desktop:~/Repos/Personal/destam-py/jobrouter$ python test.py
/home/torrin/Repos/Personal/destam-py/jobrouter/test.py:32: DeprecationWarning: There is no current event loop
  if not asyncio.get_event_loop().is_running():
usage: test.py [global_opts] cmd1 [cmd1_opts] [cmd2 [cmd2_opts] ...]
   or: test.py --help [cmd1 cmd2 ...]
   or: test.py --help-commands
   or: test.py cmd --help

error: no commands supplied
torrin@desktop:~/Repos/Personal/destam-py/jobrouter$ python test.py
/home/torrin/Repos/Personal/destam-py/jobrouter/test.py:32: DeprecationWarning: There is no current event loop
  if not asyncio.get_event_loop().is_running():
Addition Result: 13
Generated Number: 0
Generated Number: 1
Generated Number: 2
Generated Number: 3
Generated Number: 4
Generated Number: 5
torrin@desktop:~/Repos/Personal/destam-py/jobrouter$

"""

import asyncio

from jobrouter import Jobs, JobRequest, job


@job(name="addition", description="Add two numbers together")
async def addition(num1: int, num2: int):
    return num1 + num2


@job(name="number_generator", description="Generate numbers up to a given number")
async def number_generator(limit: int):
    for i in range(limit + 1):
        yield i
        await asyncio.sleep(0.1)


async def main():
    jobs = Jobs(dir="./")

    # Test 'addition' job
    job_request = JobRequest(name="addition", args={"num1": 4, "num2": 9})
    result = await jobs.router(job_request)

    print("Addition Result:", result)

    job_request = JobRequest(name="number_generator", args={"limit": 5})
    async for number in await jobs.router(job_request):
        print("Generated Number:", number)


if __name__ == "__main__":
    if not asyncio.get_event_loop().is_running():
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
