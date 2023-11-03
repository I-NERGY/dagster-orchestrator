from setuptools import find_packages, setup

setup(
    name="load_forecasting",
    packages=find_packages(exclude=["load_forecasting_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pymongo",
        "minio",
        "pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
