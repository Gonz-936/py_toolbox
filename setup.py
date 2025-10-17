# setup.py
from setuptools import setup, find_packages

setup(
    name="py_toolbox",
    version="0.7.2",  # <-- Versión incrementada
    description="Una colección de herramientas reutilizables para automatizaciones en Python.",
    author="Gonzalo Hormazabal", 
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    # 'boto3' y 'botocore' eliminados de la lista
    install_requires=[
        "google-api-python-client",
        "google-auth-httplib2",
        "google-auth-oauthlib",
        "tika",
        "beautifulsoup4",
        # pandas se instalará por separado en el Glue Job
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)