from fastapi import FastAPI
from routers.margin import router as margin_router
from routers.valuation import router as valuation_router
from routers.hsgt import router as hsgt_router

app = FastAPI(title="Stock Monitor API", version="1.0.0")

app.include_router(margin_router, prefix="/api")
app.include_router(valuation_router, prefix="/api")
app.include_router(hsgt_router, prefix="/api")


@app.get("/")
def root():
    return {
        "message": "Stock Monitor API is running",
        "docs": "/docs",
        "routes": [
            "/api/margin",
            "/api/margin/excel",
            "/api/valuation/scan",
            "/api/valuation/scan/excel",
            "/api/hsgt/fund-flow-summary",
            "/api/hsgt/fund-flow-summary/excel",
        ],
    }