from setuptools import setup


with open('requirements.txt', 'rt') as f_req:
    rows = f_req.readlines()
    reqs = []
    dep_links = []
    for req in rows:
        if not req.startswith('git+'):
            reqs.append(req)
        else:
            dep_links.append('"{}"'.format(req))

setup(
    name='nsqworker',
    packages=['nsqworker', 'locker'],
    version='0.0.1',
    install_requires=reqs,
    dependency_links=dep_links,
)
