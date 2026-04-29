from fastapi import FastAPI
from routers.margin import router as margin_router
from routers.valuation import router as valuation_router
from routers.hsgt import router as hsgt_router
from routers.market_pe import router as market_pe_router
from routers.market_turn import router as market_turn_router
from routers.industry_classification import router as industry_router
from routers.industry_turnover import router as industry_turnover_router
from routers.daily_report import router as daily_report_router
from routers.jobs import router as jobs_router

app = FastAPI(title="Stock Monitor API", version="1.0.0")

app.include_router(margin_router, prefix="/api")
app.include_router(valuation_router, prefix="/api")
app.include_router(hsgt_router, prefix="/api")
app.include_router(market_pe_router, prefix="/api")
app.include_router(market_turn_router, prefix="/api")
app.include_router(industry_router, prefix="/api")
app.include_router(industry_turnover_router, prefix="/api")
app.include_router(daily_report_router, prefix="/api")
app.include_router(jobs_router, prefix="/api")

@app.get("/")
def root():
    return {
        "message": "Stock Monitor API is running",
        "docs": "/docs",
        "routes": [
            "/api/margin",
            "/api/margin/margin-detail",
            "/api/margin/margin-detail/excel",
            "/api/valuation/scan",
            "/api/valuation/scan/excel",
            "/api/hsgt/daily-summary",
            "/api/hsgt/daily/excel",
            '/api/hsgt/hist/excel',
            '/api/market-pe/scan',
            '/api/market-pe/scan/excel',
            '/api/market-turn/scan',
            '/api/market-turn/scan/excel',
            '/api/industry-classification',
            '/api/industry-classification/excel',
            '/api/industry-turnover/scan',
            '/api/industry-turnover/scan/excel',
            '/api/daily-report/excel',
            "/api/jobs/health",
            "/api/jobs/valuation/run",
            "/api/jobs/valuation/status",
            "/api/jobs/share-structure/run",
            "/api/jobs/share-structure/status",
            "/api/jobs/market-pe/run",
            "/api/jobs/market-turn/run",
            "/api/jobs/margin/run",
            "/api/jobs/email/send",
            "/api/jobs/industruy-classification/sw/run",
            "/api/jobs/industry-turnover/run",
            "/api/jobs/daily-report/generate",

        ],
    }