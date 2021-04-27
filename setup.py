from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.10',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures', 'mdict', 'redis',
    'auguryapi @git+https://github.com/augurysys/auguryapi-py.git@0.9.34#egg=auguryapi'
    ],
)
