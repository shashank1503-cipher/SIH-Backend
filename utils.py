from PyPDF2 import PdfReader
def get_data_from_pdf(path):
    reader = PdfReader(path)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    text = text.strip()
    text = text.replace("\n", " ")
    return text
