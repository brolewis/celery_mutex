'''Packager'''
## Standard Library
import codecs
import os
## Third Party
import setuptools


def read(*parts):
    '''Read external file.'''
    here = os.path.abspath(os.path.dirname(__file__))
    return codecs.open(os.path.join(here, *parts), 'r').read()


setuptools.setup(
    name='celery_mutex',
    version='0.1.0',
    packages=setuptools.find_packages(),
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
    ],
    license='BSD License',
    author='Lewis Franklin',
    author_email='lewis.franklin@gmail.com',
    description='Mutex for Celery tasks, which can be refined.',
    long_description=read('README.md'),
    keywords='Celery tasks mutex',
    platforms='any',
    url='https://github.com/brolewis/celery_mutex',
    install_requires='kazoo>=1.0'
)
