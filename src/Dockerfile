FROM public.ecr.aws/lambda/python:3.9

RUN yum install unzip -y
COPY src/model_training.zip .
RUN unzip model_training.zip -d "${LAMBDA_TASK_ROOT}"
CMD ["lambda_function.handler"]