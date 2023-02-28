#
FROM python:latest
WORKDIR /usr/src/app
RUN pip3 install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install git+https://github.com/JustAnotherArchivist/snscrape.git
COPY . .
CMD [ "python3",  "-u", "extract/extract.py" ]
