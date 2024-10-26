from fastapi import FastAPI

app = FastAPI()

@app.get("/mult")
def multiplicate(op1: int, op2: int):
    return op1 * op2