from setuptools import setup, find_packages


setup(
    name='pure-aionsq',
    version='0.0.2',
    packages=find_packages(exclude=('tests/*',)),
    install_requires=('aiohttp>=3.3.0',),
    author='RafRaf',
    author_email='smartrafraf@gmail.com',
    description='AsyncIO version of NSQ driver',
    license='MIT',
    keywords=('nsq', 'async', 'asyncio'),
    test_suite='tests',
    url='https://github.com/RafRaf/pure-aionsq',
)
