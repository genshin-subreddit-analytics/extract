#
FROM python:latest as build
RUN python3 -m venv /venv
COPY requirements.txt requirements.txt
RUN /venv/bin/pip install -r requirements.txt

FROM python:slim AS production
WORKDIR /usr/src/app
COPY . .
COPY --from=build /venv venv

CMD [ "venv/bin/python3",  "extract/extract.py" ]
# CMD [ "/bin/bash" ]
