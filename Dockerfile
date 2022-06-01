FROM python:3.10
WORKDIR /app
RUN pip install poetry
COPY pyproject.toml poetry.lock /app/
RUN poetry install --no-dev --no-root
COPY pydht/ /app/pydht/
RUN poetry install --no-dev
EXPOSE 8000
CMD [ "poetry", "run", "pydht", "serve" ]
