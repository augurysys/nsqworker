from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.14',
    install_requires=['tornado==5.1.1', 'pynsq', 'futures; python_version == "2.7"', 'mdict', 'redis',
                      'auguryapi @ git+https://github.com/augurysys/auguryapi-py.git@0.9.39'],
)
