FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir pandas jupyter notebook

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]
