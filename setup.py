from setuptools import setup

setup(
    name='erddap_handler',
    packages=['erddap_handler'],
    include_package_data=True,
    install_requires=[
        'flask',
        'Flask-Limiter',
        'pyopenssl',
        'pandas',
        'isodate'
    ],
)