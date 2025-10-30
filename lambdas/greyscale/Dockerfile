FROM public.ecr.aws/lambda/python:3.13

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency files
COPY pyproject.toml ${LAMBDA_TASK_ROOT}/

# Install dependencies using uv
RUN uv pip install --system --no-cache -r pyproject.toml

# Copy handler code
COPY handler.py ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD ["handler.greyscale_handler"]
