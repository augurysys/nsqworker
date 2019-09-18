from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.1',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures', 'mdict', 'redis',
    'auguryapi @git+ssh://git@github.com/augurysys/auguryapi-py.git@c8e5ede91fb6fbe31f38170774379255a86a79fb#egg=auguryapi'
    ],
)
