from fastapi import APIRouter, HTTPException, File, UploadFile, Form

import configs

client = configs.client

router = APIRouter()

@router.get('/indices')
async def indices():
    