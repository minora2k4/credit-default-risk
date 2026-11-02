FROM python:3.10-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/opt/pipeline:/opt/pipeline/simulator
COPY requirements.txt /tmp/requirements.txt
# torch bản CPU (khoảng 200 MB) thay cho bản kèm CUDA (vài GB). Dùng --extra-index-url thay vì --index-url vì index
# PyTorch có metadata sai tên cho jinja2, typing-extensions nên không thay thế được PyPI; "+cpu" là phiên bản chỉ có
# ở index PyTorch nên pip không lấy nhầm bản CUDA.
RUN pip install --no-cache-dir "torch==2.14.1+cpu" --extra-index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /opt/pipeline
