from setuptools import find_packages, setup

setup(
    name="dagster_quickstart",
    packages=find_packages(exclude=["dagster_quickstart_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster_gcp",
        "google-cloud-run",
        "pandas",
        "pint",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
