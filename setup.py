#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    'thriftpy==0.3.1',
    'requests>=2.11.1',
    'xylose>=1.31.0'
]

tests_require = []

setup(
    name="articlemetaapi",
    version="1.24.0",
    description="Library that implements the endpoints of the ArticleMeta API",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    maintainer="Fabio Batalha",
    maintainer_email="fabio.batalha@scielo.org",
    url="http://github.com/scieloorg/processing",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
    ],
    dependency_links=[],
    tests_require=tests_require,
    test_suite='tests',
    install_requires=install_requires
)
