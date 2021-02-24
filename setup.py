from setuptools import setup, find_packages

setup(
    name='otello',
    version='1.0.1',
    long_description='Wrapper tool for notebook users to communicate with HySDS components',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'otello = otello.cli:cli'
        ]
    },
    install_requires=[
        'click',
        'pyyaml',
        'urllib3',
        'requests',
    ]
)
