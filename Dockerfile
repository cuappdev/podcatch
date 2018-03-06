FROM python:2.7
RUN mkdir -p /service
ADD app.py /service/app.py
ADD job.sh /service/job.sh
ADD main.py /service/main.py
ADD requirements.txt /service/requirements.txt
ADD setup.sh /service/setup.sh
ADD utils /service/utils
WORKDIR /service/
RUN pip install git+https://github.com/cuappdev/appdev.py.git --upgrade
RUN pip install -r requirements.txt
CMD python -u main.py $PODCAST_DB_USERNAME $PODCAST_DB_PASSWORD $PODCAST_DB_HOST $PODCAST_DB_NAME