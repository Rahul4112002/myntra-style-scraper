import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
worker_class = "uvicorn.workers.UvicornWorker"
workers = int(os.environ.get("WEB_CONCURRENCY", "1"))
timeout = 120
