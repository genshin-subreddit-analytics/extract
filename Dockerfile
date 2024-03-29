#
FROM python:latest as build
RUN python3 -m venv /venv
COPY requirements.txt requirements.txt
# RUN /venv/bin/pip install -r requirements.txt
RUN /venv/bin/pip install pandas
RUN /venv/bin/pip install boto3
RUN /venv/bin/pip install pyarrow
RUN /venv/bin/pip install git+https://github.com/JustAnotherArchivist/snscrape.git

FROM python:slim AS production
WORKDIR /usr/src/app
COPY . .
COPY --from=build /venv /venv

CMD [ "/venv/bin/python3",  "extract/extract.py" ]
# CMD [ "/bin/bash" ]
