# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='cm',
    version='',
    packages=find_packages(include=['cm']),
    url='',
    license_file='LICENSE.txt',
    author='crash',
    author_email='',
    description='Classical Music - internet playlist scraping and analysis',
    python_requires='>=3.10',
    install_requires=['regex',
                      'pyyaml',
                      'requests',
                      'beautifulsoup4',
                      'psycopg2-binary',
                      'sqlalchemy',
                      'levenshtein',
                      'click'],
    entry_points={
        'console_scripts': [
            'station  = cm.station:main',
            'playlist = cm.playlist:main',
            'ref_data = cm.ref_data:main',
            'database = cm.database:main'
        ],
    }
)
