from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='xTrek',
    version='0.1.0',
    author='Ivan Kiselev',
    author_email='yankoval@gmail.com',
    description='utilites for xTrek',
    packages=find_packages(),
    install_requires=requirements,
)
