from setuptools import setup

url = ""
version = "0.1.0"
readme = open('README.rst').read()

setup(name="dbluesea",
      packages=["dbluesea"],
      version=version,
      description="Store datasets in Azure storage",
      long_description=readme,
      include_package_data=True,
      author="Matthew Hartley",
      author_email="Matthew.Hartley@jic.ac.uk",
      url=url,
      install_requires=[
          "click",
          "dtoolcore",
          "pygments",
          "azure-storage"
      ],
      entry_points={
          'console_scripts': ['dbluesea=dbluesea.cli:cli']
      },
      download_url="{}/tarball/{}".format(url, version),
      license="MIT")
