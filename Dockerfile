FROM python:slim

# Python commands run inside the virtual environment
RUN python -m pip install \
        requests \
        pymongo

ADD PayoutCalculator.py /script/PayoutCalculator.py
