#!/bin/bash

docker image build -t flask-web .
docker run -p 5000:5000 -v $(pwd)/../database.db:/app/database/database.db -it --rm flask-web