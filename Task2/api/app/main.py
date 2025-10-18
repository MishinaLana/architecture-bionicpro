from typing import Union
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
import jwt
from cryptography.hazmat.primitives import serialization
from pathlib import Path
from clickhouse_driver import Client

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Создание клиента (можно вынести в отдельную функцию)
def get_clickhouse_client():
    return Client(
           host='clickhouse',
           port=9000,
           user='admin',
           password='clickhouse_password',
           database='default'
    )

security = HTTPBearer()
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    print(Path(__file__).parent)
    current_path = Path(__file__).parent
    pem_file = current_path / "public_key.pem"
    print(pem_file)
    with open(pem_file , "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read()
        )
    payload = jwt.decode(token, public_key, algorithms=["RS256"])
    return payload

def generate_json_report_for_user(email):
    import json
    client = get_clickhouse_client()
    items = client.execute(f"SELECT device_id, user_email, toString(toDateTime(created_at)) as created_at_str, telemetry FROM device_telemetry WHERE user_email='{email}'")

    return json.dumps(items, indent=2)


@app.get("/reports")
def read_root(current_user: dict = Depends(get_current_user)):

    email = current_user.get('email')

    return StreamingResponse(
       iter([generate_json_report_for_user(email)]),
       media_type="application/json",
       headers={"Content-Disposition": "attachment; filename=devices.json"}
   )
