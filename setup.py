from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.23',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures; python_version == "2.7"', 'mdict', 'redis',
                      'auguryapi @ git+https://github.com/augurysys/auguryapi-py.git@scopes-api-addition'],
)
