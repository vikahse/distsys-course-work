# FROM python:3.8-slim

# WORKDIR /02-practice-grpc/messenger

# # Copy the requirements file and download the dependencies.
# COPY server/requirements.txt .
# RUN pip install -r requirements.txt

# RUN mkdir messenger
# # COPY __init__.py messenger/__init__.py
# # COPY client/ messenger/client/
# # COPY proto messenger/proto/

# # Copy other data.
# COPY . .

# CMD ["python3", "/Users/victoriakovalevskaya/hse/distsys-course-work/homework/02-practice-grpc/messenger/server/server.py"]


FROM python:3.8-slim

WORKDIR /02-practice-grpc/messenger/
COPY server/requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir messenger
COPY __init__.py messenger/__init__.py
COPY server/ messenger/server/
COPY proto messenger/proto/

ENTRYPOINT ["python", "-m", "messenger.server.server"]
















