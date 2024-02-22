from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import boto3
import tempfile
import os
import fitz  # PyMuPDF
import asyncio

app = FastAPI()

# AWS S3 configuration
AWS_ACCESS_KEY = "ASIAZI2LIIQBD3U26VZO"
AWS_SECRET_KEY = "W9ncKndXtnrTssRGl3wjKxOo7BOnydk4HeeEasr5"
AWS_REGION = "ap-northeast-1"
S3_BUCKET_NAME = "cdk-hnb659fds-assets-637423600642-ap-northeast-1"

s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)

# AWS Lambda configuration
# LAMBDA_FUNCTION_NAME = os.environ.get("LAMBDA_FUNCTION_NAME")
# lambda_client = boto3.client("lambda", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)

def extract_text_from_pdf(pdf_path):
    text = ""
    try:
        with fitz.open(pdf_path) as pdf_document:
            num_pages = pdf_document.page_count

            for page_num in range(num_pages):
                page = pdf_document.load_page(page_num)
                text += page.get_text()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error with PyMuPDF: {str(e)}")

    return text

async def upload_to_s3_and_extract_text(file_path: str, file_name: str):
    try:
        s3_client.upload_file(file_path, S3_BUCKET_NAME, file_name)
        # lambda_client.invoke(FunctionName=LAMBDA_FUNCTION_NAME)

        if file_name.lower().endswith('.pdf'):
            text = extract_text_from_pdf(file_path)
            return text

        return True

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during file upload and text extraction: {str(e)}")

async def extract_text_background(file_path: str, file_name: str, background_tasks: BackgroundTasks):
    text = await asyncio.to_thread(upload_to_s3_and_extract_text, file_path, file_name)
    background_tasks.add_task(lambda: None)  # A placeholder task to satisfy FastAPI's requirements
    return text

@app.post("/upload")
async def handle_file_upload(file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    try:
        if not file.filename.lower().endswith(('.pdf', '.docx')):
            raise HTTPException(status_code=400, detail="Unsupported file format. Only PDF and DOCX are allowed.")

        temp_file_path = os.path.join(tempfile.gettempdir(), file.filename)
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(file.file.read())

        # Use the background task to initiate text extraction
        await extract_text_background(temp_file_path, file.filename, background_tasks)

        return JSONResponse(content={"message": "File upload and processing initiated"}, status_code=200)

    except HTTPException as e:
        raise e

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
