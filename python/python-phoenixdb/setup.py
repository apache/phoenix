from setuptools import setup, find_packages

cmdclass = {}

try:
    from sphinx.setup_command import BuildDoc
    cmdclass['build_sphinx'] = BuildDoc
except ImportError:
    pass


def readme():
    with open('README.rst') as f:
        return f.read()


version = "0.7"

setup(
    name="phoenixdb",
    version=version,
    description="Phoenix database adapter for Python",
    long_description=readme(),
    author="Lukas Lalinsky",
    author_email="lukas@oxygene.sk",
    url="https://bitbucket.org/lalinsky/python-phoenixdb",
    license="Apache 2",
    packages=find_packages(),
    include_package_data=True,
    cmdclass=cmdclass,
    command_options={
        'build_sphinx': {
            'version': ('setup.py', version),
            'release': ('setup.py', version),
        },
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    install_requires=[
        'protobuf>=3.0.0',
        'requests',
        'requests-kerberos'
    ]
)
