#
FROM python:latest
WORKDIR /usr/src/app
RUN pip3 install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
# CMD [ "python3",  "extract/extract.py" ]
CMD [ "/bin/bash" ]
