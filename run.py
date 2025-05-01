import uvicorn
from settings import config

PORT = config.APP_PARAMS['port']
HOST = config.APP_PARAMS['host']

if __name__ == "__main__":
    uvicorn.run("api.main:app", host=HOST, port=PORT, reload=True)