from setuptools import setup, find_packages

setup(
    name='etl_package',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'boto3'
    ],
    author='Vimal',
    description='ETL package for Ecommerce data processing',
)
