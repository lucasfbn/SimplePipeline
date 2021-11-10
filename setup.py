from distutils.core import setup
from setuptools import find_packages

setup(
    name='simple_pipeline',  # How you named your package folder (MyLib)
    packages=find_packages(),  # Chose the same as "name"
    version='v0.1.2',  # Start with a small number and increase it with every change you make
    license='MIT',  # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    description='',  # Give a short description about your library
    author='Lucas Fobian',  # Type in your name
    # author_email = 'your.email@domain.com',      # Type in your E-Mail
    # Provide either the link to your github or to your website
    url='https://github.com/lucasfbn/SimplePipeline',
    # I explain this later on
    download_url='https://github.com/lucasfbn/SimplePipeline/archive/refs/tags/v0.1.2.tar.gz',
    keywords=[],  # Keywords that define your package best
    install_requires=[  # I get to this in a second
        'pandas',
        'ray',
        'tqdm'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',  # Define that your audience are developers
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',  # Again, pick a license
        # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3',
    ],
)