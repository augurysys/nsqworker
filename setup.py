from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.13',
    install_requires=['tornado==5.1.1', 'pynsq', 'futures; python_version == "2.7"', 'mdict', 'redis',
                      'auguryapi @ git+https://github.com/augurysys/auguryapi-py.git@8b046e60acc8ff3ec711102d5dcd23c0b0ee950e'],
)
