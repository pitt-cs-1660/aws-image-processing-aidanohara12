FROM public.ecr.aws/lambda/python:3.13

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml .

RUN uv sync

COPY handler.py .

ENV PYTHONPATH "${LAMBDA_TASK_ROOT}/.venv/lib/python3.13/site-packages:${PYTHONPATH}"

CMD ["handler.exif_handler"]