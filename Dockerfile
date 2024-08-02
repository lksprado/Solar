FROM quay.io/astronomer/astro-runtime:11.7.0

RUN  pip install poetry
RUN poetry install --no-dev