ARG PYTHON_VERSION=3.10.8
FROM python:${PYTHON_VERSION}-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV APP_ENV=production
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

# Copy the source code into the container.
COPY . /app

# Expose the port that the application listens on.
EXPOSE 8000

# Run the application.
RUN echo "cd /app" > /app/entrypoint.sh && \
    echo "gunicorn src.app:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind=0.0.0.0:8000 --timeout 0" >> /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/bin/sh"]
CMD ["/app/entrypoint.sh"]