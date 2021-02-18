from setuptools import setup, find_packages


setup(
    name="congenial_pogato",
    version="0.0.1a",
    packages=['congenial_pogato',] + find_packages(exclude=['doc/*', 'docker/*', 'data/*', 'scripts/*', 'tests/*']),
    install_requires=['pandas',
                        'psycopg2-binary',
                      ],
)