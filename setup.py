#!/usr/bin/env python
import setuptools

install_requires = [
    'thriftpy>=0.3.9',
    'requests>=2.19.1',
    'xylose>=1.33.1'
]

tests_require = []

setuptools.setup(
    name="articlemetaapi",
    version="1.26.5",
    description="SciELO ArticleMeta SDK for Python",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    maintainer="Gustavo Fonseca",
    maintainer_email="fabio.batalha@scielo.org",
    url="http://github.com/scieloorg/articlemetaapi",
    packages=setuptools.find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    tests_require=tests_require,
    test_suite='tests',
    install_requires=install_requires,
    zip_safe=False
)
