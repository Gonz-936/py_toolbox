from setuptools import setup, find_packages

setup(
    name="py_toolbox",
    version="0.4.0", # <-- Incrementamos la versión a 0.4.0 por la nueva funcionalidad de Textract
    description="Una colección de herramientas reutilizables para automatizaciones en Python.",
    author="Gonzalo Hormazabal", 
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "boto3",
        "botocore",
        "google-api-python-client",
        "google-auth-httplib2",
        "google-auth-oauthlib",
        "tika",
        "beautifulsoup4",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)