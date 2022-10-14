FROM python:3.9

# Install nano editor just in case we need to write some file
RUN apt-get update 
RUN apt-get -y install nano 

# Install the python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all the files in the existing folder into /usr/src/app
WORKDIR D:/TKJ/IYKRA/FinalProject/Kode_Program/pubsub
COPY . .

# Export google application credentials to have the necessary permission
ENV GOOGLE_APPLICATION_CREDENTIALS="key.json"