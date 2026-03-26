# api_server.py
from fastapi import FastAPI
from pydantic import BaseModel
# from my_logic import process_data

import imgunik as img
import textfun as txt
import statfun as stt

app = FastAPI()

class ParamsRequest(BaseModel):
    params: dict

def run_service(params):
    """
    ТВОЙ СУЩЕСТВУЮЩИЙ КОД.
    Здесь ничего не меняем.
    """
    print("Запуск сервиса с params:")
    print(params)

    # имитация обработки
    params["status"] = "ok"
    return params


@app.post("/run")
def run_from_api(req: ParamsRequest):
    params_list = [req.params]

    results = []

    for params in params_list:
        result = run_service(params)
        results.append(result)

    return {
        "status": "ok",
        "result": results
    }

# class RequestData(BaseModel):
#     data: dict
#     var1: str = None
#     var2: str = None

# @app.post("/process")
# def process_endpoint(req: RequestData):
#     result = txt.create_and_process_text(params, extended_price_df, '/var/www/mnogunik.ru/proj')
#     result = process_data(req.data, req.var1, req.var2)
#     return result
