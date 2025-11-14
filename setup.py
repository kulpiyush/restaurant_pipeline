from setuptools import find_packages, setup

setup(
    name="restaurant_pipeline",
    packages=find_packages(exclude=["restaurant_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
