from setuptools import setup, find_packages

setup(
    name="lariat_python_sdk",
    version="0.1.0",
    description="A Python module to interact with Lariat API to access data quality metrics and diagnostics",
    author="Lariat Team",
    author_email="info@lariatdata.com",
    url="https://github.com/lariat-data/lariat-python-sdk/",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "requests",
        "pandas",
        "flatten-json",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD 3-Clause License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
