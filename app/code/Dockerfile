FROM python:3
ENV PYTHONUNBUFFERED 1
RUN mkdir /app
WORKDIR /app
COPY requirements_ummvar.txt /app
RUN pip install --upgrade pip
RUN pip install -r requirements_ummvar.txt
COPY . /app
EXPOSE 5000
CMD [ "python", "app_harmony.py" ]