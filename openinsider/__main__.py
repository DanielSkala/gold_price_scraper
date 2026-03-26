import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from openinsider.pipeline import run_pipeline

result = run_pipeline()
print(f"Pipeline complete: {result}")
