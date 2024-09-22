from setuptools import setup, find_packages

setup(
    name="jobrouter",
    version="0.1.2",
    packages=find_packages(),
    install_requires=[],
    author="Torrin Leonard",
    author_email="  ",
    description="A simple python jobs router",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/torrinworx/jobrouter",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)
