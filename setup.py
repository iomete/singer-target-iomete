#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name="singer-target-iomete",
    version="1.1.0",
    description="Singer.io target for loading data to iomete",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Wise",
    url='https://github.com/iomete',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only'
    ],
    py_modules=["singer_target_iomete"],
    install_requires=[
        'pipelinewise-singer-python==1.*',
        'singer==0.1.1',
        'inflection==0.5.1',
        'joblib==1.1.0',
        'boto3==1.20.20',
        'py-hive-iomete==1.1.0',
        'pytest==7.1.2',
        'python-dotenv==0.21.0',
    ],
    extras_require={
        "tests": [
            'pytest==7.1.2',
            'pyspark==3.2.1',
            'pyarrow==9.0.0',
            'python-dotenv==0.21.0'
        ]
    },
    entry_points="""
          [console_scripts]
          singer-target-iomete=singer_target_iomete:main
      """,
    packages=find_packages(exclude=['tests*']),
)
