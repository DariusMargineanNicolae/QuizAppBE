from fastapi import FastAPI, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from typing import Optional


import uvicorn

from utils import get_questions_from_api

app = FastAPI(debug=True)


class QuizAppParams(BaseModel):
    amount: Optional[int] = None
    # Add other parameters here as needed


router = APIRouter()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/hello_world")
async def hello_world():
    return "hello_world"


@app.get("/get_questions")
async def get_questions(
    amount: Optional[int] = Query(5),
    category: Optional[str] = None,
    difficulty: Optional[str] = None,
):
    quizz_app_params = {"amount": amount}

    if category:
        quizz_app_params["category"] = category
    if difficulty:
        quizz_app_params["difficulty"] = difficulty

    questions = get_questions_from_api(params=quizz_app_params)
    return questions


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
