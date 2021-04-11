import io
import re
from os.path import dirname
from os.path import join

from setuptools import find_packages, setup


def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ).read()


project_name = 'spotify-mood'
module_name = project_name
regex_found_badges = re.compile('^.. start-badges.*^.. end-badges', re.M | re.S)
dependencies = [
    'pyspark==3.0.1',
    'pyarrow==2.0.0',
    'spotipy',
    'psycopg2-binary',
    'py4j==0.10.9',
    'psycopg2',
    'numpy==1.19.5',
    'pandas',
    'scikit-learn'
]

setup(
    name=module_name,
    version='0.1.0-SNAPSHOT',
    author='Damavis',
    author_email='tboutaour@gmail.com',
    long_description='Spotify mood',
    python_requires='>=3.7',
    test_suite='nose.collector',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages('src', exclude=['tests', 'tests.*']),
    package_dir={'': 'src'},
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Operating System :: Unix',
    ],
    tests_require=["nose", "spark-testing-base", "mock_repository"] + dependencies,
    install_requires=dependencies)
