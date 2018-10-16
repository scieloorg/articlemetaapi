#!/usr/bin/env python
from setuptools import setup

install_requires = [
    'thriftpy>=0.3.9',
    'requests>=2.19.1',
    'xylose>=1.33.1'
]

tests_require = []

setup(
    name="articlemetaapi",
    version="1.26.4",
    description="Library that implements the endpoints of the ArticleMeta API",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    maintainer="Fabio Batalha",
    maintainer_email="fabio.batalha@scielo.org",
    url="http://github.com/scieloorg/articlemetaapi",
    packages=['articlemeta'],
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
    ],
    tests_require=tests_require,
    test_suite='tests',
    install_requires=install_requires,
    zip_safe=False
)
