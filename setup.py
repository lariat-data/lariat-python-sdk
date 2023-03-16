from setuptools import setup, find_packages

setup(
    name="lariat_public_api",
    version="0.0.1",
    author="Lariat Team",
    author_email="info@lariatdata.com",
    description="This package contains common lariat python libraries",
    url="https://github.com/lariat-data/lariat-public-api/",
    packages=[
        "lariat_public_api",
        "lariat_public_api.client",
    ],
    install_requires=["flatten_json", "requests", "pandas"],
)
