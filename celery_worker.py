from app import celery

celery.conf.update(
    broker_url="redis://localhost:6379/0",
    result_backend="redis://localhost:6379/0"
)

if __name__ == "__main__":
    celery.start()
