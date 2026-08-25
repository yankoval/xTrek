from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='xtrek',
    version='0.1.1',
    author='Ivan Kiselev',
    author_email='yankoval@gmail.com',
    description='utilities for xTrek',
    python_requires='>=3.9',
    packages=find_packages(include=['xtrek', 'xtrek.*', 'amica', 'amica.*']),
    install_requires=requirements,
    extras_require={
        'reports': ['Jinja2>=3.1'],
        'reports-pdf': ['Jinja2>=3.1', 'WeasyPrint>=62'],
    },
    entry_points={
        'console_scripts': [
            'nk=xtrek.nk:main',
            'suz=xtrek.suz:main',
            'intersect=xtrek.intersect:main',
            'intersect-gui=xtrek.IntersectGUI:main',
            'trueapi=xtrek.trueapi:main',
            'gs1-processor=xtrek.gs1_processor:main',
            'kiz-from-rep=xtrek.kiz_from_rep:main',
            'crpt-auth=xtrek.crpt_auth:main',
            'xtrek-prn=xtrek.prn_util:main',
            'vbg=xtrek.vbg:main',
        ],
    },
    package_data={
        'xtrek': [
            '*.json',
            'my_orgs/*.json',
            'reports/templates/html/*.jinja',
            'reports/templates/markdown/*.jinja',
            'reports/styles/*.css',
            'reports/styles/profiles/*.css',
            'reports/styles/themes/*.css',
        ],
    },
)
