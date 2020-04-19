 
#docker build -f Dockerfile -t "cloud_naive_airflow" .
#docker run -p 8000:8000 --name aircloud -t cloud_naive_airflow

FROM python:3.6

EXPOSE 8000

COPY requirements.txt appv1.py model.py ./
RUN python -m pip install -r requirements.txt

CMD gunicorn --bind 0.0.0.0:8000 appv1:app
