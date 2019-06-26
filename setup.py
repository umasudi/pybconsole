from setuptools import setup

CLASSIFIERS = [
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.6',
]

TESTS_REQUIRES = [
    'mock'
]

setup(
    name='bconsole',
    version='0.6.13',
    description='Bacula console python implementation',
    url='https://github.com/umasudi/pybconsole',
    author='Anton Dmitrenok',
    author_email='avdmitrenok@gmail.com',
    license='GPLv3',
    packages=['bconsole'],
    tests_require=TESTS_REQUIRES,
    classifiers=CLASSIFIERS,
    zip_safe=False
)
