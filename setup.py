from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.1',
    install_requires=['tornado==4.5.3', 'pynsq', 'futures', 'mdict', 'redis'],
)
