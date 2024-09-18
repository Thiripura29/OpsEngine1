from setuptools import setup, find_namespace_packages

requirements = []
with open('requirements.txt', 'r') as file:
    for line in file:
        line = line.strip()
        requirements.append(line.strip())

print(requirements)
# Minimal example for versioning purposes, not ready yet.
setup(
    name="OpsEngine",
    version="0.1",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[requirements],
    python_requires=">=3.7",
    include_package_data=True,
)
