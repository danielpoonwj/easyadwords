from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'googleads',
    'unicodecsv'
]

setup(
    name='easyadwords',
    version='0.1.1',
    description="User friendly wrapper for Google AdWords",
    long_description=readme + '\n\n' + history,
    author="Daniel Poon",
    author_email='daniel.poon.wenjie@gmail.com',
    url='https://github.com/danielpoonwj/easyadwords',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords=['google', 'adwords', 'wrapper'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ]
)
