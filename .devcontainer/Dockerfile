FROM mcr.microsoft.com/devcontainers/python:3.14-bookworm

# Match pyproject.toml's Python >=3.14.2,<3.15 and the GitHub Actions
# Poetry 2.3 toolchain. Do not run project tests on a 3.11 image.
RUN python3 -c "import sys; assert (3, 14, 2) <= sys.version_info[:3] < (3, 15), sys.version"
RUN python3 -m pip install 'poetry~=2.3'
