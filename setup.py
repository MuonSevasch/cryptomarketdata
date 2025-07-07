from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="cryptomd",
    version="0.2.1",
    author="Arseny Zaitsev",
    author_email="arseny.zaitsev@gmail.com",
    description="Lib for cryptomarket data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MuonSevasch/cryptomarketdata",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)