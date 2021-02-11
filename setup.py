from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.2',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures', 'mdict', 'redis',
    'git+ssh://git@github.com/augurysys/auguryapi-py.git@577f2cbdecca0bae3419252497d1d38070416a7d#egg=auguryapi'
    ],
)
