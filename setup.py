from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.13',
    install_requires=['tornado==5.1.1', 'pynsq', 'futures; python_version == "2.7"', 'mdict', 'redis',
                      'auguryapi @ git+https://github.com/augurysys/auguryapi-py.git'
                      '@53bc054fd72ad09178d20e4d1160641c7a0c46ad'
                      ],
)
