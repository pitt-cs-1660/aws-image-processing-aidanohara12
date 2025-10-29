FROM public.ecr.aws/lambda/python:3.13

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy project files
COPY pyproject.toml ${LAMBDA_TASK_ROOT}/
COPY handler.py ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN uv pip install --system --no-cache -r pyproject.toml

# Set the CMD to your handler
CMD ["handler.resize_handler"]
