"""API Gateway adapter for the existing FastAPI upload service.

The handler does not reimplement CSV rules or presigning. Mangum translates
HTTP API events into the same application that Docker runs locally, including
the upload page.
"""

from mangum import Mangum

from backend.app.main import app

handler = Mangum(app, lifespan="off")
