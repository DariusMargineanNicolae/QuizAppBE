from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware

import uvicorn


app = FastAPI(debug=True)

router = APIRouter()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
