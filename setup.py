from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.1',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures', 'mdict', 'redis',
    'auguryapi @git+ssh://git@github.com/augurysys/auguryapi-py.git@1d5537c7a454e6ca96424a54effddbe022f6a09b#egg=auguryapi'
    ],
)
