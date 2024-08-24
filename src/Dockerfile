FROM public.ecr.aws/lambda/python:3.9

RUN yum install unzip -y
COPY src/forecast_metrics.zip .
RUN unzip forecast_metrics.zip -d "${LAMBDA_TASK_ROOT}"
CMD ["lambda_function.handler"]