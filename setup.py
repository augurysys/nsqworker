from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.11',
    install_requires=['tornado==4.5.3', 'pynsq', 'mdict', 'redis',
    'auguryapi @git+https://github.com/augurysys/auguryapi-py.git@0.9.34#egg=auguryapi'
    ],
)
