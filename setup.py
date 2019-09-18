from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.1',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures', 'mdict', 'redis',
    'auguryapi @git+ssh://git@github.com/augurysys/auguryapi-py.git@395b1ce0269d9805406660e44c11c37e6fc9dee1#egg=auguryapi'
    ],
)
