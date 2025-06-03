FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --upgrade -r requirements.txt

# Remove py which is pulled in by retry, py is not needed and is a CVE
RUN pip uninstall -y py

COPY ./sifthub ${LAMBDA_TASK_ROOT}/sifthub

# Lambda handler for SQS-triggered export processing
CMD ["sifthub.reporting.event.listener.sqs_listener.handler"]
