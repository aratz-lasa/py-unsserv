import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = [
    "rpcudp==4.0.0",
]

extras_require = {
    "dev": [
        "pytest==5.4.1",
        "pytest-asyncio==0.11.0",
        "mypy==0.740",
        "flake8==3.7.9",
        "black==19.10b0",
    ],
}

setuptools.setup(
    name="unsserv",
    version="0.0.2",
    author="Aratz M. Lasa",
    author_email="aratz.m.lasa@gmail.com",
    description="P2P high-level library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aratz-lasa/py-unsserv",
    install_requires=install_requires,
    extras_require=extras_require,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
