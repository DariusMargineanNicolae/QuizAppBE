import requests


def get_questions_from_api(params: dict):

    url = "https://opentdb.com/api.php"

    response = requests.get(url, params=params)  # pylint: disable=missing-timeout
    questions = response.json()
    print(questions)
    return questions
