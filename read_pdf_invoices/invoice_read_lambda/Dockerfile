FROM public.ecr.aws/lambda/python:3.11

RUN yum install -y poppler-utils

WORKDIR ${LAMBDA_TASK_ROOT}

ADD requirements-lambda.txt /tmp
RUN pip install -t ${LAMBDA_TASK_ROOT} -r /tmp/requirements-lambda.txt
# --quiet 
# Copy requirements.txt
#COPY requirements-lambda.txt ${LAMBDA_TASK_ROOT}
# Install the specified packages
#RUN pip install -r requirements-lambda.txt

# Copy function code
COPY lambda-read-invoice.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda-read-invoice.lambda_handler" ]