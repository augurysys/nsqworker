from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.13',
    install_requires=['tornado==5.1.1', 'pynsq', 'futures; python_version == "2.7"', 'mdict', 'redis',
                      'auguryapi @ git+https://github.com/augurysys/auguryapi-py.git'
                      '@ab7b15ec849c33c87ab4ebfa7e8bee67350bda69'
                      ],
)
