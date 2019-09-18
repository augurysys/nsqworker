from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.1',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures', 'mdict', 'redis',
    'auguryapi @git+ssh://git@github.com/augurysys/auguryapi-py.git@7e29797ff6abb30f372ce4950a1e53cca05da85e#egg=auguryapi'
    ],
)
