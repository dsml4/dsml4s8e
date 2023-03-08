from setuptools import setup
import dsml4s8e

setup(
    name='dsml4s8e',
    version=dsml4s8e.__version__,
    description='build daster pipelines from standalone notebooks',
    url='https://github.com/dsml4/dsml4s8e',
    author='Aleksandr Motuzov',
    author_email='motuzov@gmail.com',
    license='Apache 2.0',
    packages=['dsml4s8e'],
    install_requires=['dagster',
                      'dagstermill'
                      ],

    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
