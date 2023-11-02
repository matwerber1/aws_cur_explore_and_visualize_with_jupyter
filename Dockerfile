FROM jupyter/datascience-notebook:latest

# INSTALL AWS CLI
USER root
# If using x86/Intel/AMD:
#curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
# If using ARM:
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN rm awscliv2.zip
RUN rm -r ./aws
USER jovyan

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
