from setuptools import setup, find_packages

dependencies = """
BeautifulSoup
simplejson
"""

setup(
    name='electionarchive',
    version='0.1',
    description='Project to archive data related to elections in India',
    packages=find_packages(),
    install_requires=dependencies.split(),
)    
